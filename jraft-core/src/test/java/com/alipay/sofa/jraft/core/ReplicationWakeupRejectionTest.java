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
package com.alipay.sofa.jraft.core;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.test.TestUtils;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.ThreadPoolsFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * PATCH A/B regression anchor (integration level).
 *
 * Verifies end-to-end that leader->follower entry replication survives when the
 * group closure pool rejects every new-log wakeup dispatch. The 200ms pauses
 * between batches let the replicators drain and park in waitMoreEntries, so each
 * subsequent batch hits the exact "parked -> wakeup -> rejected" incident path.
 *
 * A/B contract:
 * - On an UNPATCHED build (LogManagerImpl without the rescue executor) this test
 *   MUST go red: the first rejected wakeup permanently orphans the replicators,
 *   quorum never advances and the batch times out.
 * - On the patched build it MUST be green: every rejected dispatch is rescued.
 */
public class ReplicationWakeupRejectionTest {

    private String dataPath;

    @Before
    public void setup() throws Exception {
        this.dataPath = TestUtils.mkTempDir();
        FileUtils.forceMkdir(new File(this.dataPath));
    }

    @After
    public void teardown() throws Exception {
        if (!TestCluster.CLUSTERS.isEmpty()) {
            for (final TestCluster c : TestCluster.CLUSTERS.removeAll()) {
                c.stopAll();
            }
        }
        NodeManager.getInstance().clear();
        FileUtils.deleteDirectory(new File(this.dataPath));
    }

    /**
     * Rejects ONLY dispatches coming from wakeupAllWaiter / notifyOnNewLog, lets
     * everything else (disk closures, FSM events, shutdown publishes) through, so
     * the cluster stays functional except for the wakeup path under test.
     */
    private static ThreadPoolExecutor wakeupRejectingPool() {
        return new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory(
            "it-selective-reject-", true)) {

            @Override
            public void execute(final Runnable command) {
                for (final StackTraceElement e : Thread.currentThread().getStackTrace()) {
                    if ("wakeupAllWaiter".equals(e.getMethodName()) || "notifyOnNewLog".equals(e.getMethodName())) {
                        throw new RejectedExecutionException("it: reject wakeup dispatch");
                    }
                }
                super.execute(command);
            }
        };
    }

    /** registerThreadPool has no unregister API: make it idempotent for repeated runs in one JVM. */
    private static void registerQuietly(final String groupId, final ThreadPoolExecutor pool) {
        try {
            ThreadPoolsFactory.registerThreadPool(groupId, pool);
        } catch (final IllegalArgumentException ignored) {
            // Already registered by a previous run in this JVM, same pool behavior.
        }
    }

    @Test
    public void testReplicationSurvivesWakeupRejection() throws Exception {
        final String groupId = "it-rescue-cluster";
        final List<PeerId> peers = TestUtils.generatePeers(3);
        final TestCluster cluster = new TestCluster(groupId, this.dataPath, peers);
        for (final PeerId peer : peers) {
            assertTrue(cluster.start(peer.getEndpoint()));
        }
        cluster.waitLeader();
        final Node leader = cluster.getLeader();
        assertNotNull(leader);

        // Baseline: 10 tasks through the normal pool, cluster is healthy.
        sendTestTasks(leader, 10);

        // From now on EVERY new-log wakeup dispatch is rejected: replication only
        // survives if the rescue executor delivers the wakeups.
        registerQuietly(groupId, wakeupRejectingPool());

        // 5 batches with 200ms pauses: the pauses let the replicators drain and
        // park, so each batch re-enters the "parked -> wakeup" path.
        for (int b = 0; b < 5; b++) {
            sendTestTasks(leader, 10);
            Thread.sleep(200);
        }
        cluster.ensureSame();
        cluster.stopAll();
    }

    private void sendTestTasks(final Node leader, final int n) throws Exception {
        final CountDownLatch latch = new CountDownLatch(n);
        for (int i = 0; i < n; i++) {
            leader.apply(new Task(ByteBuffer.wrap(("data" + i).getBytes()), new ExpectClosure(latch)));
        }
        assertTrue("tasks not committed within 10s: replication stalled (a wakeup was lost)",
            latch.await(10, TimeUnit.SECONDS));
    }
}
