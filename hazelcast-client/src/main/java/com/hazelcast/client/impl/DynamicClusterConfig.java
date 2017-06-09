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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddListConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddLockConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec;
import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.EntryListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.ItemListenerConfigHolder;
import com.hazelcast.client.impl.protocol.task.dynamicconfig.RingbufferStoreConfigHolder;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.ItemListenerConfig;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * //todo describe this class
 */
public class DynamicClusterConfig extends Config {

    private final HazelcastClientInstanceImpl instance;
    private final SerializationService serializationService;

    public DynamicClusterConfig(HazelcastClientInstanceImpl instance) {
        this.instance = instance;
        this.serializationService = instance.getSerializationService();
    }

    @Override
    public Config addMapConfig(MapConfig mapConfig) {
        return super.addMapConfig(mapConfig);
    }

    @Override
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        return super.addCacheConfig(cacheConfig);
    }

    @Override
    public Config addQueueConfig(QueueConfig queueConfig) {
        return super.addQueueConfig(queueConfig);
    }

    @Override
    public Config addLockConfig(LockConfig lockConfig) {
        ClientMessage request = DynamicConfigAddLockConfigCodec.encodeRequest(lockConfig.getName(), lockConfig.getQuorumName());
        invoke(request);
        return this;
    }

    @Override
    public Config addListConfig(ListConfig listConfig) {
        List<ItemListenerConfigHolder> listenerConfigs = null;
        if (!listConfig.getItemListenerConfigs().isEmpty()) {
            listenerConfigs = new ArrayList<ItemListenerConfigHolder>();
            for (ItemListenerConfig listenerConfig : listConfig.getItemListenerConfigs()) {
                listenerConfigs.add(ItemListenerConfigHolder.of(listenerConfig, instance.getSerializationService()));
            }
        }
        ClientMessage request = DynamicConfigAddListConfigCodec.encodeRequest(listConfig.getName(), listenerConfigs,
                listConfig.getBackupCount(), listConfig.getAsyncBackupCount(), listConfig.getMaxSize(), listConfig.isStatisticsEnabled());
        invoke(request);
        return this;
    }

    @Override
    public Config addSetConfig(SetConfig setConfig) {
        return super.addSetConfig(setConfig);
    }

    @Override
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        List<EntryListenerConfigHolder> listenerConfigHolders = null;
        List<EntryListenerConfig> entryListenerConfigs = multiMapConfig.getEntryListenerConfigs();
        if (entryListenerConfigs != null && !entryListenerConfigs.isEmpty()) {
            listenerConfigHolders = new ArrayList<EntryListenerConfigHolder>(entryListenerConfigs.size());
            for (EntryListenerConfig entryListenerConfig : entryListenerConfigs) {
                listenerConfigHolders.add(EntryListenerConfigHolder.of(entryListenerConfig,
                        instance.getSerializationService()));
            }
        }

        ClientMessage request = DynamicConfigAddMultiMapConfigCodec.encodeRequest(
                multiMapConfig.getName(), multiMapConfig.getValueCollectionType().toString(),
                listenerConfigHolders,
                multiMapConfig.isBinary(), multiMapConfig.getBackupCount(), multiMapConfig.getAsyncBackupCount(),
                multiMapConfig.isStatisticsEnabled());
        invoke(request);
        return this;
    }

    @Override
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        return super.addReplicatedMapConfig(replicatedMapConfig);
    }

    @Override
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        RingbufferStoreConfigHolder ringbufferStoreConfig = null;
        if (ringbufferConfig.getRingbufferStoreConfig() != null &&
                ringbufferConfig.getRingbufferStoreConfig().isEnabled()) {
            RingbufferStoreConfig storeConfig = ringbufferConfig.getRingbufferStoreConfig();
            ringbufferStoreConfig = RingbufferStoreConfigHolder.of(storeConfig, instance.getSerializationService());
        }
        ClientMessage request = DynamicConfigAddRingbufferConfigCodec.encodeRequest(
                ringbufferConfig.getName(), ringbufferConfig.getCapacity(), ringbufferConfig.getBackupCount(),
                ringbufferConfig.getAsyncBackupCount(), ringbufferConfig.getTimeToLiveSeconds(),
                ringbufferConfig.getInMemoryFormat().name(), ringbufferStoreConfig);
        invoke(request);
        return this;
    }

    @Override
    public Config addTopicConfig(TopicConfig topicConfig) {
        return super.addTopicConfig(topicConfig);
    }

    @Override
    public Config addReliableTopicConfig(ReliableTopicConfig topicConfig) {
        return super.addReliableTopicConfig(topicConfig);
    }

    @Override
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        ClientMessage request = DynamicConfigAddExecutorConfigCodec.encodeRequest(
                executorConfig.getName(), executorConfig.getPoolSize(), executorConfig.getQueueCapacity(),
                executorConfig.isStatisticsEnabled());
        invoke(request);
        return this;
    }

    @Override
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        ClientMessage request = DynamicConfigAddDurableExecutorConfigCodec.encodeRequest(
                durableExecutorConfig.getName(), durableExecutorConfig.getPoolSize(),
                durableExecutorConfig.getDurability(), durableExecutorConfig.getCapacity());
        invoke(request);
        return this;
    }

    @Override
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        ClientMessage request = DynamicConfigAddScheduledExecutorConfigCodec.encodeRequest(
                scheduledExecutorConfig.getName(), scheduledExecutorConfig.getPoolSize(),
                scheduledExecutorConfig.getDurability(), scheduledExecutorConfig.getCapacity());
        invoke(request);
        return this;
    }

    @Override
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        ClientMessage request = DynamicConfigAddCardinalityEstimatorConfigCodec.encodeRequest(
                cardinalityEstimatorConfig.getName(), cardinalityEstimatorConfig.getBackupCount(),
                cardinalityEstimatorConfig.getAsyncBackupCount());
        invoke(request);
        return this;
    }

    @Override
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        return super.addSemaphoreConfig(semaphoreConfig);
    }

    @Override
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        return super.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    public Config addJobTrackerConfig(JobTrackerConfig jobTrackerConfig) {
        return super.addJobTrackerConfig(jobTrackerConfig);
    }

    @Override
    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        return super.addQuorumConfig(quorumConfig);
    }

    @Override
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        return super.addListenerConfig(listenerConfig);
    }

    @Override
    public ClassLoader getClassLoader() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setClassLoader(ClassLoader classLoader) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ConfigPatternMatcher getConfigPatternMatcher() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public String getProperty(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setProperty(String name, String value) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Properties getProperties() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setProperties(Properties properties) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public String getInstanceName() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setInstanceName(String instanceName) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public GroupConfig getGroupConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setGroupConfig(GroupConfig groupConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public NetworkConfig getNetworkConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public MapConfig findMapConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public MapConfig getMapConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public CacheSimpleConfig findCacheConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public LockConfig findLockConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public LockConfig getLockConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ListConfig findListConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ListConfig getListConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public SetConfig findSetConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public SetConfig getSetConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setReplicatedMapConfigs(Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setReliableTopicConfigs(Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setTopicConfigs(Map<String, TopicConfig> mapTopicConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setDurableExecutorConfigs(Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setScheduledExecutorConfigs(Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setCardinalityEstimatorConfigs(
            Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public SemaphoreConfig getSemaphoreConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public WanReplicationConfig getWanReplicationConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setWanReplicationConfigs(Map<String, WanReplicationConfig> wanReplicationConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public JobTrackerConfig findJobTrackerConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public JobTrackerConfig getJobTrackerConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, JobTrackerConfig> getJobTrackerConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setJobTrackerConfigs(Map<String, JobTrackerConfig> jobTrackerConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Map<String, QuorumConfig> getQuorumConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public QuorumConfig getQuorumConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public QuorumConfig findQuorumConfig(String name) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ManagementCenterConfig getManagementCenterConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ServicesConfig getServicesConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setServicesConfig(ServicesConfig servicesConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public SecurityConfig getSecurityConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public List<ListenerConfig> getListenerConfigs() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public SerializationConfig getSerializationConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public PartitionGroupConfig getPartitionGroupConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ManagedContext getManagedContext() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setManagedContext(ManagedContext managedContext) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public NativeMemoryConfig getNativeMemoryConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public URL getConfigurationUrl() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setConfigurationUrl(URL configurationUrl) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public File getConfigurationFile() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setConfigurationFile(File configurationFile) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public String getLicenseKey() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setLicenseKey(String licenseKey) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public boolean isLiteMember() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setLiteMember(boolean liteMember) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        throw new UnsupportedOperationException("This config object only supports adding new configuration.");
    }

    @Override
    public String toString() {
        return "DynamicClusterConfic{instance=" + instance + "}";
    }

    private void invoke(ClientMessage request) {
        try {
            ClientInvocation invocation = new ClientInvocation(instance, request);
            ClientInvocationFuture future = invocation.invoke();
            ClientMessage response = future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }
}
