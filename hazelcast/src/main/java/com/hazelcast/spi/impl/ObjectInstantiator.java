/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.ClassLoaderUtil;

/**
 * Creates and initializes instances of objects.
 *
 * We have plenty of space where we are instantiating an object and then inject HazelcastInstance.
 * ObjectInstantiator encapsulates this functionality
 *
 */
public final class ObjectInstantiator {
    private final HazelcastInstance hazelcastInstance;
    private final ClassLoader configClassLoader;

    public ObjectInstantiator(HazelcastInstance hazelcastInstance, ClassLoader configClassLoader) {
        this.hazelcastInstance = hazelcastInstance;
        this.configClassLoader = configClassLoader;
    }

    /**
     * Create a new instance of given class.
     *
     * @param className
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> T newInitializedInstance(String className) throws Exception {
        T newInstance = ClassLoaderUtil.newInstance(configClassLoader, className);
        if (newInstance instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) newInstance).setHazelcastInstance(hazelcastInstance);
        }
        return newInstance;
    }
}
