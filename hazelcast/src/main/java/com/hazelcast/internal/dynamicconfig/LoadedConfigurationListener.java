package com.hazelcast.internal.dynamicconfig;

public interface LoadedConfigurationListener {
    /**
     * Called after a node loads a configuration of a data-structure from a persistence store.
     *
     * @param serviceName   service this configuration belongs to
     * @param name          name of a data-structure
     * @param config        the actual configuration object
     */
    void onConfigurationLoaded(String serviceName, String name, Object config);
}
