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

package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MultiMapConfig.ValueCollectionType.LIST;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DynamicConfigTest extends HazelcastTestSupport {

    protected static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] members;
    // dynamic configuration is added on driver instance
    private HazelcastInstance driver;

    @Before
    public void setup() {
        members = newInstances();
        driver = getDriver();
    }

    protected HazelcastInstance[] newInstances() {
        factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        HazelcastInstance[] instances = factory.newInstances();
        return instances;
    }

    protected HazelcastInstance getDriver() {
        return members[members.length - 1];
    }

    @Test
    public void testMultiMapConfig() {
        String mapName = randomString();
        MultiMapConfig multiMapConfig = new MultiMapConfig(mapName);
        multiMapConfig.setBackupCount(4)
                      .setAsyncBackupCount(2)
                      .setStatisticsEnabled(true)
                      .setBinary(true)
                      .setValueCollectionType(LIST)
                      .addEntryListenerConfig(
                              new EntryListenerConfig("com.hazelcast.Listener", true, false)
                      );

        driver.getConfig().addMultiMapConfig(multiMapConfig);

        MultiMapConfig configOnCluster = getConfigurationService().getMultiMapConfig(mapName);
        assertEquals(multiMapConfig.getName(), configOnCluster.getName());
        assertEquals(multiMapConfig.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(multiMapConfig.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
        assertEquals(multiMapConfig.isStatisticsEnabled(), configOnCluster.isStatisticsEnabled());
        assertEquals(multiMapConfig.isBinary(), configOnCluster.isBinary());
        assertEquals(multiMapConfig.getValueCollectionType(), configOnCluster.getValueCollectionType());
        assertEquals(multiMapConfig.getEntryListenerConfigs().get(0),
                configOnCluster.getEntryListenerConfigs().get(0));
    }

    @Test
    public void testCardinalityEstimatorConfig() {
        String name = randomString();
        CardinalityEstimatorConfig config = new CardinalityEstimatorConfig(name, 4 ,2);

        driver.getConfig().addCardinalityEstimatorConfig(config);

        CardinalityEstimatorConfig configOnCluster = getConfigurationService().getCardinalityEstimatorConfig(name);
        assertEquals(config.getName(), configOnCluster.getName());
        assertEquals(config.getBackupCount(), configOnCluster.getBackupCount());
        assertEquals(config.getAsyncBackupCount(), configOnCluster.getAsyncBackupCount());
    }

    private ConfigurationService getConfigurationService() {
        return getNodeEngineImpl(members[members.length - 1]).getConfigurationService();
    }
}
