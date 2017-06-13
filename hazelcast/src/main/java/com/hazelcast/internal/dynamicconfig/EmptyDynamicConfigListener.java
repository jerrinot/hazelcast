package com.hazelcast.internal.dynamicconfig;

import com.hazelcast.config.MapConfig;


/**
 * Empty implementation of {@link DynamicConfigListener}
 *
 */
public class EmptyDynamicConfigListener implements DynamicConfigListener {

    @Override
    public void onConfigRegistered(MapConfig configObject) {
        //intentionally no-op
    }

    @Override
    public void onServiceInitialized(ClusterWideConfigurationService configurationService) {
        //intentionally no-op
    }
}
