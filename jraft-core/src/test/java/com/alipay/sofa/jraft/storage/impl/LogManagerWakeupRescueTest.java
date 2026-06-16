/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.storage.impl;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.BaseStorageTest;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolsFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * PATCH regression tests: a new-log wakeup dispatch must never be
 * silently lost, neither when the group closure pool rejects the dispatch (variant A,
 * rescued by NEW_LOG_WAKEUP_RESCUE_EXECUTOR) nor when the callback itself throws
 * (variant B, logged loudly and retried once on the rescue executor).
 */
@RunWith(value = MockitoJUnitRunner.class)
public class LogManagerWakeupRescueTest extends BaseStorageTest {

    private static final String  RESCUE_THREAD_PREFIX = "JRaft-NewLog-Wakeup-Rescue-";

    private LogManagerImpl       logManager;
    private ConfigurationManager confManager;
    @Mock
    private FSMCaller            fsmCaller;
    private LogStorage           logStorage;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @After
    public void teardown() throws Exception {
        if (this.logStorage != null) {
            this.logStorage.shutdown();
        }
        super.teardown();
    }

    /**
     * Rejects ONLY dispatches coming from wakeupAllWaiter / notifyOnNewLog, lets
     * everything else through. This avoids breaking LogManagerImpl's other
     * runInThread usages (e.g. the diskQueue shutdown publish), so the test cases
     * and teardown can still complete. It works because runInThread -> submit ->
     * execute happens synchronously on the dispatching thread, so the dispatching
     * frames are visible on the current stack.
     */
    private static ThreadPoolExecutor wakeupRejectingPool() {
        return new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory(
            "ut-selective-reject-", true)) {

            @Override
            public void execute(final Runnable command) {
                for (final StackTraceElement e : Thread.currentThread().getStackTrace()) {
                    if ("wakeupAllWaiter".equals(e.getMethodName()) || "notifyOnNewLog".equals(e.getMethodName())) {
                        throw new RejectedExecutionException("ut: reject wakeup dispatch");
                    }
                }
                super.execute(command);
            }
        };
    }

    /**
     * registerThreadPool rejects duplicate registration for the same groupId and
     * there is no unregister API: make it idempotent for repeated runs in one JVM.
     */
    private static void registerQuietly(final String groupId, final ThreadPoolExecutor pool) {
        try {
            ThreadPoolsFactory.registerThreadPool(groupId, pool);
        } catch (final IllegalArgumentException ignored) {
            // Already registered by a previous run in this JVM, same pool behavior.
        }
    }

    /** Each case uses its own groupId to avoid cross-pollution via the static registry. */
    private void init(final String groupId) {
        this.confManager = new ConfigurationManager();
        final RaftOptions raftOptions = new RaftOptions();
        this.logStorage = new RocksDBLogStorage(this.path, raftOptions);
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(new NodeMetrics(false));
        opts.setLogStorage(this.logStorage);
        opts.setRaftOptions(raftOptions);
        opts.setGroupId(groupId);
        assertTrue(this.logManager.init(opts));
    }

    /** Same as LogManagerTest#mockAddEntries: append 10 entries and wait for them to be stable. */
    private void mockAddEntries() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        this.logManager.appendEntries(new ArrayList<>(TestUtils.mockEntries(10)), new LogManager.StableClosure() {

            @Override
            public void run(final Status status) {
                assertTrue(status.isOk());
                latch.countDown();
            }
        });
        latch.await();
    }

    /** UT-1: wakeupAllWaiter dispatch rejected -> rescued (variant A, main path). */
    @Test
    public void testWakeupRejectedFallsBackToRescue() throws Exception {
        final String groupId = "ut-rescue-wakeup";
        registerQuietly(groupId, wakeupRejectingPool());
        init(groupId);

        mockAddEntries(); // lastLogIndex = 10
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<String> cbThread = new AtomicReference<>();
        final long waitId = this.logManager.wait(10, (arg, errorCode) -> {
            cbThread.set(Thread.currentThread().getName());
            assertEquals(RaftError.SUCCESS.getNumber(), errorCode);
            latch.countDown();
            return true;
        }, null);
        assertTrue(waitId > 0); // registered (parked state)

        mockAddEntries(); // new logs -> wakeup dispatch rejected -> rescue
        assertTrue("wakeup lost: the rescue patch is not effective", latch.await(5, TimeUnit.SECONDS));
        assertTrue("callback should run on the rescue thread, actual: " + cbThread.get(), cbThread.get()
            .startsWith(RESCUE_THREAD_PREFIX));
        assertFalse(this.logManager.removeWaiter(waitId)); // waiter already consumed
    }

    /** UT-2: notifyOnNewLog immediate dispatch rejected -> rescued, and MUST be async (variant A, secondary path). */
    @Test
    public void testNotifyRejectedFallsBackToRescue() throws Exception {
        final String groupId = "ut-rescue-notify";
        registerQuietly(groupId, wakeupRejectingPool());
        init(groupId);

        mockAddEntries(); // lastLogIndex = 10
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<String> cbThread = new AtomicReference<>();
        // expected=5 != 10 -> notifyOnNewLog takes the immediate-dispatch branch -> rejected -> rescue
        final long waitId = this.logManager.wait(5, (arg, errorCode) -> {
            cbThread.set(Thread.currentThread().getName());
            latch.countDown();
            return true;
        }, null);
        assertEquals(0, waitId); // not registered, immediate dispatch
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // Key assertion: must run on the rescue thread (async). Running inline on the
        // calling thread would violate the ThreadId lock-ordering constraint and
        // recreate the orphan bug this patch fixes.
        assertTrue("callback must run asynchronously (lock-ordering constraint), actual: " + cbThread.get(), cbThread
            .get().startsWith(RESCUE_THREAD_PREFIX));
    }

    /** UT-3: callback throws once -> retried once on rescue and succeeds (variant B, transient failure). */
    @Test
    public void testCallbackThrowingOnceIsRetriedOnRescue() throws Exception {
        init("ut-retry-once"); // no registered pool: uses the default global pool
        mockAddEntries();
        final AtomicInteger calls = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);
        this.logManager.wait(10, (arg, errorCode) -> {
            if (calls.incrementAndGet() == 1) {
                throw new RuntimeException("ut: transient failure");
            }
            latch.countDown();
            return true;
        }, null);
        mockAddEntries();
        assertTrue("retry did not happen: variant B rescue is not effective", latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, calls.get());
    }

    /** UT-4: callback always throws -> total attempts strictly bounded to 2, no infinite retry. */
    @Test
    public void testCallbackThrowingTwiceGivesUpBounded() throws Exception {
        init("ut-retry-bounded");
        mockAddEntries();
        final AtomicInteger calls = new AtomicInteger();
        this.logManager.wait(10, (arg, errorCode) -> {
            calls.incrementAndGet();
            throw new RuntimeException("ut: deterministic failure");
        }, null);
        mockAddEntries();
        Thread.sleep(1000);
        assertEquals("retry must be bounded (first attempt + one retry)", 2, calls.get());
        Thread.sleep(500);
        assertEquals(2, calls.get()); // confirm it stops growing
    }

    /** UT-5: happy path regression guard: normal dispatch never touches the rescue thread, runs exactly once. */
    @Test
    public void testHappyPathNotOnRescueThread() throws Exception {
        init("ut-happy");
        mockAddEntries();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<String> cbThread = new AtomicReference<>();
        this.logManager.wait(10, (arg, errorCode) -> {
            calls.incrementAndGet();
            cbThread.set(Thread.currentThread().getName());
            latch.countDown();
            return true;
        }, null);
        mockAddEntries();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, calls.get());
        assertFalse("happy path must not use the rescue thread", cbThread.get().startsWith(RESCUE_THREAD_PREFIX));
    }

    /** UT-6: ALL waiters are rescued, not just the first one (one rejection must not kill both replicators). */
    @Test
    public void testAllWaitersRescuedNotJustFirst() throws Exception {
        final String groupId = "ut-rescue-all";
        registerQuietly(groupId, wakeupRejectingPool());
        init(groupId);
        mockAddEntries();
        final CountDownLatch latch = new CountDownLatch(2); // simulates the waiters of two replicators
        this.logManager.wait(10, (arg, e) -> {
            latch.countDown();
            return true;
        }, null);
        this.logManager.wait(10, (arg, e) -> {
            latch.countDown();
            return true;
        }, null);
        mockAddEntries();
        // Unpatched: the first rejected submit aborts the dispatch loop and both waiters are lost.
        assertTrue("some waiter was lost", latch.await(5, TimeUnit.SECONDS));
    }

    /** UT-7: the ESTOP wakeup on shutdown is also delivered through rescue when rejected. */
    @Test
    public void testShutdownEstopDeliveredViaRescue() throws Exception {
        final String groupId = "ut-rescue-estop";
        registerQuietly(groupId, wakeupRejectingPool());
        init(groupId);
        mockAddEntries();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger code = new AtomicInteger(-1);
        this.logManager.wait(10, (arg, errorCode) -> {
            code.set(errorCode);
            latch.countDown();
            return true;
        }, null);
        this.logManager.shutdown(); // stopped=true -> wakeupAllWaiter(ESTOP) -> rejected -> rescue
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(RaftError.ESTOP.getNumber(), code.get());
    }
}
