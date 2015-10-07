/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.bufferpool;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.util.ConcurrentReferenceHashMap;

import java.util.concurrent.ConcurrentMap;

/**
 * A thread-local like structure for {@link BufferPoolImpl}.
 *
 * The reason not a regular ThreadLocal is being used is that the pooled instances should be
 * pooled per HZ instance because:
 * - buffers can contain references to hz instance specifics
 * - buffers for a given hz instance should be cleanable when the instance shuts down and we don't
 * want to have memory leaks.
 */
public final class BufferPoolThreadLocal {

    private final SerializationService serializationService;
    private final BufferPoolFactory bufferPoolFactory;
    private final ThreadLocal<BufferPool> pools = new ThreadLocal<BufferPool>() {
        @Override
        protected BufferPool initialValue() {
            return bufferPoolFactory.create(serializationService);
        }
    };

    public BufferPoolThreadLocal(SerializationService serializationService, BufferPoolFactory bufferPoolFactory) {
        this.serializationService = serializationService;
        this.bufferPoolFactory = bufferPoolFactory;
    }

    public BufferPool get() {
        return pools.get();
    }

    public void clear() {
        //todo: is this needed?
    }
}
