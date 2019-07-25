/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.core.metrics;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobMetrics;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.test.HazelcastSerialClassRunner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.core.JetTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
public class JobMetricsStressTest extends JetTestSupport {

    private static final int JET_INSTANCE_COUNT = 2;
    private static final int RESTART_COUNT = 100;
    private static final int SUSPEND_RESUME_COUNT = 50;
    private static final int TOTAL_PROCESSORS = Runtime.getRuntime().availableProcessors() * JET_INSTANCE_COUNT;

    private static Throwable restartThreadException;
    private static Throwable obtainMetricsThreadException;

    private static long previousMetricValueSum;

    private JetInstance instance;

    @Before
    public void setup() throws Exception {
        restartThreadException = null;
        obtainMetricsThreadException = null;
        instance = createJetMembers(JET_INSTANCE_COUNT)[0];
    }

    @Test
    public void restart_stressTest() throws Throwable {
        stressTest(job -> new JobRestartThread(job), RESTART_COUNT);
    }

    @Test
    public void suspend_resume_stressTest() throws Throwable {
        stressTest(job -> new JobSuspendResumeThread(job), SUSPEND_RESUME_COUNT);
    }

    private void stressTest(Function<Job, Runnable> restart, int restartCount) throws Throwable {
        DAG dag = buildDag();
        Job job = instance.newJob(dag);
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));

        ObtainMetricsThread obtainMetrics = new ObtainMetricsThread(job);
        Thread restartThread = new Thread(restart.apply(job));
        Thread obtainThread = new Thread(obtainMetrics);

        restartThread.start();
        obtainThread.start();

        restartThread.join();
        obtainMetrics.stop = true;
        obtainThread.join();

        if (restartThreadException != null) {
            throw restartThreadException;
        }
        if (obtainMetricsThreadException != null) {
            throw obtainMetricsThreadException;
        }

        assertEquals(restartCount * TOTAL_PROCESSORS * TOTAL_PROCESSORS, previousMetricValueSum);

        job.cancel();
    }

    private DAG buildDag() {
        DAG dag = new DAG();
        dag.newVertex("p", (SupplierEx<Processor>) IncrementingProcessor::new);
        return dag;
    }

    public static final class IncrementingProcessor implements Processor {

        @Probe
        public static final AtomicInteger COUNT = new AtomicInteger();

        @Override
        public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
            COUNT.incrementAndGet();
        }

        @Override
        public boolean complete() {
            return false;
        }
    }

    private static class JobRestartThread implements Runnable {

        private final Job job;
        private int restartCount;

        JobRestartThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (restartCount < RESTART_COUNT) {
                    job.restart();
                    restartCount++;
                    assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
                }
            } catch (Throwable ex) {
                restartThreadException = ex;
                throw ex;
            }
        }
    }

    private static class JobSuspendResumeThread implements Runnable {

        private final Job job;
        private int restartCount;

        JobSuspendResumeThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (restartCount < SUSPEND_RESUME_COUNT) {
                    job.suspend();
                    sleepMillis(100);
                    assertTrueEventually(() -> assertEquals(JobStatus.SUSPENDED, job.getStatus()));
                    job.resume();
                    sleepMillis(100);
                    restartCount++;
                    assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));
                }
            } catch (Throwable ex) {
                restartThreadException = ex;
                throw ex;
            }
        }
    }

    private static class ObtainMetricsThread implements Runnable {

        public boolean stop;
        private final Job job;

        ObtainMetricsThread(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (!stop) {
                    assertNotNull(job.getMetrics());
                    JobMetrics withTag = job.getMetrics().withTag("metric", "COUNT");
                    Set<String> metricNames = withTag.getMetricNames();
//                    if (previousMetricValueSum > 0) {
//                        assertTrue(!metricNames.isEmpty());
//                    }
                    if (metricNames.isEmpty()) {
                        continue;
//                    } else {
//                        assertEquals(TOTAL_PROCESSORS, metricNames.size());
                    }
                    long sum = 0;
                    for (String metricName : metricNames) {
                        sum += withTag.getMetricValue(metricName);
                    }
                    assertTrue("Metrics value should be increasing, current: " + sum
                            + ", previous: " + previousMetricValueSum,
                            sum >= previousMetricValueSum);
                    previousMetricValueSum = sum;
                }
            } catch (Throwable ex) {
                obtainMetricsThreadException = ex;
                throw ex;
            }
        }
    }
}
