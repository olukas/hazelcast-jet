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

package com.hazelcast.jet.pipeline.test;

import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
public class FreezingTest {

    @ClassRule
    public static Timeout globalTimeout = Timeout.seconds(180);

    @AfterClass
    public static void afterClass() {
        try {
            Thread.sleep(7_200_000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println("after");
    }

    @After
    public void after() {
        System.out.println("after");
    }

    @Test
    public void test() {
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println("test1");
    }

    @Test
    public void test2() {
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println("test2");
    }

    @Test
    public void test3() {
        try {
            Thread.sleep(5_000);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        System.out.println("test3");
    }
}
