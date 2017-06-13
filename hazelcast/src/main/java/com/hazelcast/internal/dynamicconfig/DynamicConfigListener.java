package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.MapConfig;

/**
 * Listener to be notified about various events in {@link ClusterWideConfigurationService}
 *
 */
public interface DynamicConfigListener {
    /**
     * Called when a {@link ClusterWideConfigurationService} is initialized. It allows to hook custom hooks.
     *
     * @param configurationService
     */
    void onServiceInitialized(ClusterWideConfigurationService configurationService);

    /**
     * Called when a new {@link MapConfig} object is created locally.
     *
     * @param configObject
     */
    void onConfigRegistered(MapConfig configObject);
}
