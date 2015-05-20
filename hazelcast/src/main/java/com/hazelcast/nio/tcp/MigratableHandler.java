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

package com.hazelcast.nio.tcp;

/**
 * Read/Write handlers that supports migration between {@link com.hazelcast.nio.tcp.IOSelector}.
 *
 */
public interface MigratableHandler extends SelectionHandler {

    /**
     * Requests the MigratableHandler to move to the new owner.
     *
     * @param newOwner
     */
    void requestMigration(IOSelector newOwner);

    /**
     * Get IOSelector currently owning this handler. Handler owner is a thread running this handler.
     * {@link com.hazelcast.nio.tcp.handlermigration.IOBalancer IOBalancer} can decide to migrate
     * a handler to another owner.
     *
     * @return current owner
     */
    IOSelector getOwner();

    /**
     * Get number of events recorded by the current handler. It can be used to calculate whether
     * this handler should be migrated to a different {@link com.hazelcast.nio.tcp.IOSelector}
     *
     * @return total number of events recorded by this handler
     */
    long getEventCount();
}
