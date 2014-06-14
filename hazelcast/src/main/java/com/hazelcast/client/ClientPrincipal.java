/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.UUID;

public final class ClientPrincipal implements Portable {

    private UUID uuid;
    private UUID ownerUuid;

    public ClientPrincipal() {
    }

    public ClientPrincipal(UUID uuid, UUID ownerUuid) {
        this.uuid = uuid;
        this.ownerUuid = ownerUuid;
    }

    public UUID getUuid() {
        return uuid;
    }

    public UUID getOwnerUuid() {
        return ownerUuid;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.PRINCIPAL;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("uuid-least", uuid.getLeastSignificantBits());
        writer.writeLong("uuid-most", uuid.getMostSignificantBits());
        writer.writeLong("ownerUuid-least", ownerUuid.getLeastSignificantBits());
        writer.writeLong("ownerUuid-most", ownerUuid.getMostSignificantBits());
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        long leastSig = reader.readLong("uuid-least");
        long mostSig = reader.readLong("uuid-most");
        uuid = new UUID(mostSig, leastSig);
        leastSig = reader.readLong("ownerUuid-least");
        mostSig = reader.readLong("ownerUuid-most");
        ownerUuid = new UUID(mostSig, leastSig);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientPrincipal that = (ClientPrincipal) o;

        if (ownerUuid != null ? !ownerUuid.equals(that.ownerUuid) : that.ownerUuid != null) {
            return false;
        }
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + (ownerUuid != null ? ownerUuid.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientPrincipal{");
        sb.append("uuid='").append(uuid).append('\'');
        sb.append(", ownerUuid='").append(ownerUuid).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
