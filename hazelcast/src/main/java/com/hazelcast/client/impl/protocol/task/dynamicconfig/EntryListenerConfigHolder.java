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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

public class EntryListenerConfigHolder extends ListenerConfigHolder {

    private final boolean local;
    private final boolean includeValue;

    public EntryListenerConfigHolder(String className, boolean local, boolean includeValue) {
        super(className);
        this.local = local;
        this.includeValue = includeValue;
    }

    public EntryListenerConfigHolder(Data listenerImpl, boolean local, boolean includeValue) {
        super(listenerImpl);
        this.local = local;
        this.includeValue = includeValue;
    }

    public boolean isLocal() {
        return local;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public EntryListenerConfig asEntryListenerConfig(SerializationService serializationService) {
        validate();

        EntryListenerConfig entryListenerConfig;
        if (className != null) {
            entryListenerConfig = new EntryListenerConfig(className, local, includeValue);
        } else {
            Object listenerImplementation = serializationService.toObject(this.listenerImplementation);
            if (listenerImplementation instanceof MapListener) {
                entryListenerConfig = new EntryListenerConfig((MapListener) this.listenerImplementation, local, includeValue);
            } else if (listenerImplementation instanceof EntryListener) {
                entryListenerConfig = new EntryListenerConfig((EntryListener) this.listenerImplementation, local, includeValue);
            } else {
                throw new IllegalArgumentException("Entry listener has to be an instance of MapListener or EntryListener");
            }
        }
        return entryListenerConfig;
    }
}
