/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.internal.dynamicconfig.ClusterWideConfigurationService;
import com.hazelcast.internal.dynamicconfig.DynamicConfigSmokeTest;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class DynamicClusterConfigTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance[] members;
    private HazelcastInstance client;

    @Before
    public void setup() {
        members = factory.newInstances(getConfig(), 2);
        client = factory.newHazelcastClient();
    }

    @Test
    public void multimap_smokeMultimap_initialTest() {
        String mapName = "dynamicMM";

        MultiMapConfig dynamicMultimapConfig = new MultiMapConfig(mapName);
        dynamicMultimapConfig.setBackupCount(3);
        dynamicMultimapConfig.addEntryListenerConfig(new EntryListenerConfig(new DynamicConfigSmokeTest.DummyEntryListener(), true, true));
        Config dynamicConfig1 = client.getConfig();
        dynamicConfig1.addMultiMapConfig(dynamicMultimapConfig);

        MultiMap<String, String> multiMap = members[0].getMultiMap(mapName);
        multiMap.put("foo", "1");

        ClusterWideConfigurationService configurationService = getNodeEngineImpl(members[0]).getService(ClusterWideConfigurationService.SERVICE_NAME);
        MultiMapConfig dynamicallyAddedConfig = configurationService.getMultiMapConfig(mapName);
        assertEquals(3, dynamicallyAddedConfig.getBackupCount());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }
}
