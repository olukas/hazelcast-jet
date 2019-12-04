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

package com.hazelcast.jet.impl.observer;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.ReliableMessageListener;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.LongSupplier;

import static com.hazelcast.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;

public class ObservableRepository {

    /**
     * Prefix of all topic names used to back {@link Observable} implementations,
     * necessary so that such topics can't clash with regular topics used
     * for other purposes.
     */
    private static final String JET_OBSERVABLE_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "observable.";

    private static final String COMPLETED_OBSERVABLES_LIST_NAME = INTERNAL_JET_OBJECTS_PREFIX + "completedObservables";
    private static final int MAX_CLEANUP_ATTEMPTS_AT_ONCE = 10;

    private final JetInstance jet;
    private final IList<Tuple2<String, Long>> completedObservables;
    private final long expirationTime;
    private final LongSupplier timeSource;

    public ObservableRepository(JetInstance jet, JetConfig config) {
        this(jet, config, System::currentTimeMillis);
    }

    ObservableRepository(JetInstance jet, JetConfig config, LongSupplier timeSource) {
        this.jet = jet;
        this.completedObservables = jet.getList(COMPLETED_OBSERVABLES_LIST_NAME);
        this.expirationTime = getExpirationTime(config);
        this.timeSource = timeSource;
    }

    @SuppressWarnings("unchecked")
    public static void registerObservable(Observable observable, JetInstance jet) {
        ITopic topic = getTopic(jet.getHazelcastInstance(), observable.name());
        topic.addMessageListener((ReliableMessageListener) observable);
    }

    public static FunctionEx<HazelcastInstance, ConsumerEx<ArrayList<Object>>> getPublishFn(String name) {
        return instance -> {
            ITopic<ObservableBatch> topic = getTopic(instance, name);
            return buffer -> {
                topic.publish(ObservableBatch.items(buffer));
                buffer.clear();
            };
        };
    }

    public static void completeObservable(String observable, Throwable error, JetInstance jet) {
        completeObservable(observable, error, jet, System::currentTimeMillis);
    }

    static void completeObservable(String observable, Throwable error, JetInstance jet, LongSupplier timeSource) {
        ITopic<ObservableBatch> topic = getTopic(jet.getHazelcastInstance(), observable);
        topic.publish(error == null ? ObservableBatch.endOfData() : ObservableBatch.error(error));

        IList<Tuple2<String, Long>> completedObservables =
                jet.getList(ObservableRepository.COMPLETED_OBSERVABLES_LIST_NAME);
        completedObservables.add(Tuple2.tuple2(observable, timeSource.getAsLong()));
    }

    private static long getExpirationTime(JetConfig jetConfig) {
        //we will keep observables for the same amount of time as job results
        HazelcastProperties hazelcastProperties = new HazelcastProperties(jetConfig.getProperties());
        return hazelcastProperties.getMillis(JetProperties.JOB_RESULTS_TTL_SECONDS);
    }

    @Nonnull
    private static ITopic<ObservableBatch> getTopic(HazelcastInstance intance, String observableName) {
        String topicName = JET_OBSERVABLE_NAME_PREFIX + observableName;
        return intance.getReliableTopic(topicName);
    }

    public void cleanup() {
        long currentTime = timeSource.getAsLong();
        int cleaned = 0;
        Iterator<Tuple2<String, Long>> iterator = completedObservables.iterator();
        while (iterator.hasNext() && cleaned < MAX_CLEANUP_ATTEMPTS_AT_ONCE) {
            Tuple2<String, Long> tuple2 = iterator.next();
            long completionTime = tuple2.getValue();
            boolean expired = currentTime - completionTime >= expirationTime;
            if (expired) {
                getTopic(jet.getHazelcastInstance(), tuple2.getKey()).destroy();
                iterator.remove();
                cleaned++;
            } else {
                return;
            }
        }
    }

}