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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.TestInClusterSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JobMetrics_BatchTest extends TestInClusterSupport {

    private static final String SOURCE_VERTEX = "items";
    private static final String FLAT_MAP_AND_FILTER_VERTEX = "fused(flat-map, filter)";
    private static final String GROUP_AND_AGGREGATE_PREPARE_VERTEX = "group-and-aggregate-prepare";
    private static final String GROUP_AND_AGGREGATE_VERTEX = "group-and-aggregate";
    private static final String SINK_VERTEX = "mapSink(counts)";
    private static final String RECEIVE_COUNT_METRIC = "receivedCount";
    private static final String EMITTED_COUNT_METRIC = "emittedCount";

    private static final String COMMON_TEXT = "look at some common text here and uncommon text here";

    @Test
    public void when_jobCompleted_then_metricsExist() {
        Pipeline p = createPipeline();

        Job job = testMode.getJet().newJob(p);
        // When
        job.join();

        // Then
        assertTrueEventually(() -> assertMetrics(job.getMetrics()));
    }

    @Test
    public void when_memberAddedAfterJobFinished_then_metricsNotAffected() {
        Pipeline p = createPipeline();

        Job job = testMode.getJet().newJob(p);
        job.join();

        // When
        JetInstance newMember = factory.newMember(prepareConfig());
        try {
            assertTrueEventually(() -> assertEquals(MEMBER_COUNT + 1, newMember.getCluster().getMembers().size()));
            // Then
            assertTrueEventually(() -> assertMetrics(job.getMetrics()));
        } finally {
            newMember.shutdown();
        }
    }

    @Test
    public void when_memberRemovedAfterJobFinished_then_metricsNotAffected() {
        Pipeline p = createPipeline();

        JetInstance newMember = factory.newMember(prepareConfig());
        Job job;
        try {
            assertTrueEventually(() -> assertEquals(MEMBER_COUNT + 1, newMember.getCluster().getMembers().size()));
            job = testMode.getJet().newJob(p);
            job.join();
        } finally {
            newMember.shutdown();
        }
        assertTrueEventually(() -> assertEquals(MEMBER_COUNT, testMode.getJet().getCluster().getMembers().size()));
        assertTrueEventually(() -> assertMetrics(job.getMetrics()));
    }

    @Test
    public void when_twoDifferentPipelines_then_haveDifferentMetrics() {
        String anotherText = "look at some common text here and here";
        Pipeline p = createPipeline();
        Pipeline p2 = createPipeline(anotherText);

        Job job = testMode.getJet().newJob(p);
        Job job2 = testMode.getJet().newJob(p2);
        job.join();
        job2.join();

        assertNotEquals(job.getMetrics(), job2.getMetrics());
        assertTrueEventually(() -> assertMetrics(job.getMetrics()));
        assertTrueEventually(() -> assertMetrics(job2.getMetrics(), anotherText));
    }

    @Test
    public void when_twoDifferentJobsForTheSamePipeline_then_haveDifferentMetrics() {
        Pipeline p = createPipeline();

        Job job = testMode.getJet().newJob(p);
        Job job2 = testMode.getJet().newJob(p);
        job.join();
        job2.join();

        assertNotEquals(job.getMetrics(), job2.getMetrics());
        assertTrueEventually(() -> assertMetrics(job.getMetrics()));
        assertTrueEventually(() -> assertMetrics(job2.getMetrics()));
    }

    private Pipeline createPipeline() {
        return createPipeline(COMMON_TEXT);
    }

    private Pipeline createPipeline(String text) {
        Pipeline p = Pipeline.create();
        p.drawFrom(TestSources.items(text))
                .flatMap(line -> traverseArray(line.toLowerCase().split("\\W+")))
                .filter(word -> !word.isEmpty())
                .groupingKey(wholeItem())
                .aggregate(counting())
                .drainTo(Sinks.map("counts"));
        return p;
    }

    private void assertMetrics(JobMetrics metrics) {
        assertMetrics(metrics, COMMON_TEXT);
    }

    private void assertMetrics(JobMetrics metrics, String originalText) {
        Assert.assertNotNull(metrics);

        String[] words = originalText.split("\\W+");
        int wordCount = words.length;
        int uniqueWordCount = new HashSet<>(asList(words)).size();

        assertEquals(1, sumValueFor(metrics, SOURCE_VERTEX, EMITTED_COUNT_METRIC));
        assertEquals(1, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, RECEIVE_COUNT_METRIC));
        assertEquals(wordCount, sumValueFor(metrics, FLAT_MAP_AND_FILTER_VERTEX, EMITTED_COUNT_METRIC));
        assertEquals(wordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_PREPARE_VERTEX, RECEIVE_COUNT_METRIC));
        assertEquals(uniqueWordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_PREPARE_VERTEX, EMITTED_COUNT_METRIC));
        assertEquals(uniqueWordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_VERTEX, RECEIVE_COUNT_METRIC));
        assertEquals(uniqueWordCount, sumValueFor(metrics, GROUP_AND_AGGREGATE_VERTEX, EMITTED_COUNT_METRIC));
        assertEquals(uniqueWordCount, sumValueFor(metrics, SINK_VERTEX, RECEIVE_COUNT_METRIC));
    }

    private long sumValueFor(JobMetrics metrics, String vertex, String metric) {
        Collection<Measurement> measurements = metrics
                .filter(MeasurementPredicates.tagValueEquals(MetricTags.VERTEX, vertex))
                .get(metric);
        return measurements.stream().mapToLong(Measurement::getValue).sum();
    }

}