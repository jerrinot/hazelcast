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

package com.hazelcast.config;

import com.hazelcast.internal.dynamicconfig.AddDynamicConfigOperation;
import com.hazelcast.internal.dynamicconfig.DynamicConfigReplicationOperation;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.map.eviction.LFUEvictionPolicy;
import com.hazelcast.map.eviction.LRUEvictionPolicy;
import com.hazelcast.map.eviction.RandomEvictionPolicy;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.CONFIG_DS_FACTORY_ID;

/**
 * DataSerializerHook for com.hazelcast.config classes
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class ConfigDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CONFIG_DS_FACTORY, CONFIG_DS_FACTORY_ID);

    public static final int WAN_REPLICATION_CONFIG = 0;
    public static final int WAN_CONSUMER_CONFIG = 1;
    public static final int WAN_PUBLISHER_CONFIG = 2;
    public static final int NEAR_CACHE_CONFIG = 3;
    public static final int NEAR_CACHE_PRELOADER_CONFIG = 4;
    public static final int ADD_DYNAMIC_CONFIG_OP = 5;
    public static final int REPLICATE_CONFIGURATIONS_OP = 6;
    public static final int MULTIMAP_CONFIG = 7;
    public static final int LISTENER_CONFIG = 8;
    public static final int ENTRY_LISTENER_CONFIG = 9;
    public static final int MAP_CONFIG = 10;
    public static final int RANDOM_EVICTION_POLICY = 11;
    public static final int LFU_EVICTION_POLICY = 12;
    public static final int LRU_EVICTION_POLICY = 13;
    public static final int MAP_STORE_CONFIG = 14;
    public static final int MAP_PARTITION_LOST_LISTENER_CONFIG = 15;
    public static final int MAP_INDEX_CONFIG = 16;
    public static final int MAP_ATTRIBUTE_CONFIG = 17;
    public static final int QUERY_CACHE_CONFIG = 18;
    public static final int PREDICATE_CONFIG = 19;
    public static final int PARTITION_STRATEGY_CONFIG = 20;
    public static final int HOT_RESTART_CONFIG = 21;
    public static final int TOPIC_CONFIG = 22;
    public static final int RELIABLE_TOPIC_CONFIG = 23;
    public static final int ITEM_LISTENER_CONFIG = 24;
    public static final int QUEUE_STORE_CONFIG = 25;
    public static final int QUEUE_CONFIG = 26;
    public static final int LOCK_CONFIG = 27;
    public static final int LIST_CONFIG = 28;
    public static final int SET_CONFIG = 29;
    public static final int EXECUTOR_CONFIG = 30;


    private static final int LEN = EXECUTOR_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[WAN_REPLICATION_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanReplicationConfig();
            }
        };
        constructors[WAN_CONSUMER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanConsumerConfig();
            }
        };
        constructors[WAN_PUBLISHER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new WanPublisherConfig();
            }
        };
        constructors[NEAR_CACHE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NearCacheConfig();
            }
        };
        constructors[NEAR_CACHE_PRELOADER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new NearCachePreloaderConfig();
            }
        };
        constructors[ADD_DYNAMIC_CONFIG_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddDynamicConfigOperation();
            }
        };
        constructors[REPLICATE_CONFIGURATIONS_OP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DynamicConfigReplicationOperation();
            }
        };
        constructors[MULTIMAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MultiMapConfig();
            }
        };
        constructors[LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListenerConfig();
            }
        };
        constructors[ENTRY_LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new EntryListenerConfig();
            }
        };
        constructors[MAP_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapConfig();
            }
        };
        constructors[RANDOM_EVICTION_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new RandomEvictionPolicy();
            }
        };
        constructors[LFU_EVICTION_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LFUEvictionPolicy();
            }
        };
        constructors[LRU_EVICTION_POLICY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LRUEvictionPolicy();
            }
        };
        constructors[MAP_STORE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapStoreConfig();
            }
        };
        constructors[MAP_PARTITION_LOST_LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapPartitionLostListenerConfig();
            }
        };
        constructors[MAP_INDEX_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapIndexConfig();
            }
        };
        constructors[MAP_ATTRIBUTE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new MapAttributeConfig();
            }
        };
        constructors[QUERY_CACHE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueryCacheConfig();
            }
        };
        constructors[PREDICATE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PredicateConfig();
            }
        };
        constructors[PARTITION_STRATEGY_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PartitioningStrategyConfig();
            }
        };
        constructors[HOT_RESTART_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new HotRestartConfig();
            }
        };
        constructors[TOPIC_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new TopicConfig();
            }
        };
        constructors[RELIABLE_TOPIC_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReliableTopicConfig();
            }
        };
        constructors[ITEM_LISTENER_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ItemListenerConfig();
            }
        };
        constructors[QUEUE_STORE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueStoreConfig();
            }
        };
        constructors[QUEUE_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new QueueConfig();
            }
        };
        constructors[LOCK_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LockConfig();
            }
        };
        constructors[LIST_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListConfig();
            }
        };
        constructors[SET_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetConfig();
            }
        };
        constructors[EXECUTOR_CONFIG] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ExecutorConfig();
            }
        };



        return new ArrayDataSerializableFactory(constructors);
    }
}
