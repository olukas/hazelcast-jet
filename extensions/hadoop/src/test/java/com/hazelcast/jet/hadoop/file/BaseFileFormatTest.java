/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.file;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@RunWith(Parameterized.class)
public abstract class BaseFileFormatTest extends JetTestSupport {

    @Parameter
    public boolean useHadoop;

    protected String currentDir;

    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("Start setup with " + useHadoop);
        currentDir = Paths.get(".").toAbsolutePath().normalize().toString();
        if (useHadoop) {
            assumeThatNoWindowsOS();
        }
    }

    @SafeVarargs
    protected final <T> void assertItemsInSource(FileSourceBuilder<T> source, T... items) {
        assertItemsInSource(source, collected -> assertThat(collected).containsOnly(items));
    }

    protected <T> void assertItemsInSource(
            FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        assertItemsInSource(1, source, assertion);
    }

    protected <T> void assertItemsInSource(
            int memberCount, FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        System.out.println("Before build pipeline with " + useHadoop);
        BatchSource<T> build = source.build();
        p.readFrom(build)
                .apply(Assertions.assertCollected(assertion));
        System.out.println("Start Jet with " + useHadoop);
        JetInstance[] jets = createJetMembers(memberCount);
        System.out.println("Started Jet with " + useHadoop);
        try {
            System.out.println("Start job with " + useHadoop);
            jets[0].newJob(p).join();
            System.out.println("Finished job with " + useHadoop);
        } finally {
            for (JetInstance jet : jets) {
                jet.shutdown();
            }
        }
    }

    protected void assertJobFailed(FileSourceBuilder source, Class expectedRootException, String expectedMessage) {
        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .writeTo(Sinks.logger());

        JetInstance[] jets = createJetMembers(1);

        try {
            assertThatThrownBy(() -> jets[0].newJob(p).join())
                    .hasCauseInstanceOf(JetException.class)
                    .hasRootCauseInstanceOf(expectedRootException)
                    .hasMessageContaining(expectedMessage);
        } finally {
            for (JetInstance jet : jets) {
                jet.shutdown();
            }
        }
    }
}
