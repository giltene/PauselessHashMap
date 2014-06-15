/*
 * Copyright 2014 Diogo Ferreira
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.giltene;

import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.TestStringMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Tests the map using the guava test harness.
 *
 * @author Diogo Ferreira (diogo.ferreira@feedzai.com)
 */
public class PauselessHashMapGuavaTest {
    public static Test suite() {
        return new PauselessHashMapGuavaTest().allTests();
    }

    public Test allTests() {
        TestSuite suite = new TestSuite("org.giltene PauselessHashMap");
        suite.addTest(testsForPauselessHashMap());
        return suite;
    }

    public Test testsForPauselessHashMap() {
        return MapTestSuiteBuilder
                .using(new TestStringMapGenerator() {
                    @Override
                    protected Map<String, String> create(
                            Map.Entry<String, String>[] entries) {
                        return toHashMap(entries);
                    }
                })
                .named("HashMap")
                .withFeatures(
                        MapFeature.GENERAL_PURPOSE,
                        MapFeature.ALLOWS_NULL_KEYS,
                        MapFeature.ALLOWS_NULL_VALUES,
                        MapFeature.ALLOWS_ANY_NULL_QUERIES,
                        MapFeature.FAILS_FAST_ON_CONCURRENT_MODIFICATION,
                        CollectionFeature.SUPPORTS_ITERATOR_REMOVE,
                        CollectionFeature.SERIALIZABLE,
                        CollectionSize.ANY)
                .suppressing(suppressForHashMap())
                .createTestSuite();
    }

    private static Map<String, String> toHashMap(
            Map.Entry<String, String>[] entries) {
        return populate(new PauselessHashMap<String, String>(), entries);
    }

    private static <T, M extends Map<T, String>> M populate(
            M map, Map.Entry<T, String>[] entries) {
        for (Map.Entry<T, String> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    protected Collection<Method> suppressForHashMap() {
        return Collections.emptySet();
    }
}
