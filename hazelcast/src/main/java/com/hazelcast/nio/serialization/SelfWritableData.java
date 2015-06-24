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

package com.hazelcast.nio.serialization;

import java.nio.ByteBuffer;

/**
 * Sub-type of {@link com.hazelcast.nio.serialization.Data} to represent self-writable data.
 * Self-Writable means writes itself to given destination.
 *
 * @see Data
 */
public interface SelfWritableData extends Data {

    /**
     * Writes itself to given {@link java.nio.ByteBuffer}.
     *
     * @param destination   the {@link java.nio.ByteBuffer} to write itself into
     * @param offset        the offset within this data of the first byte to be read and write to destination.
     * @param length        the number of bytes to be read from this data and write to destination
     *
     * @return the written {@link java.nio.ByteBuffer}
     *
     * @throws {@link java.nio.BufferOverflowException} if there is insufficient space in the destination buffer
     *
     * @throws {@link java.lang.IndexOutOfBoundsException} if the <code>offset</code> and <code>length</code>
     *         points the outside of this data
     *
     * @throws {@link java.nio.ReadOnlyBufferException} if the destination buffer is read-only
     */
    ByteBuffer writeTo(ByteBuffer destination, int offset, int length);

}
