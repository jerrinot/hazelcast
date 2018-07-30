package com.hazelcast.internal.json;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JSON_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JSON_DS_FACTORY_ID;

public class JsonFactoryHook implements DataSerializerHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(JSON_DS_FACTORY, JSON_DS_FACTORY_ID);

    public static final int STRING = 0;
    public static final int NUMBER = 1;
    public static final int OBJECT = 2;
    public static final int ARRAY = 3;
    public static final int LITERAL = 4;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case STRING:
                        return new JsonString();
                    case NUMBER:
                        return new JsonNumber();
                    case OBJECT:
                        return new JsonObject();
                    case ARRAY:
                        return new JsonArray();
                    case LITERAL:
                        return new JsonLiteral();
                    default:
                        return null;
                }
            }
        };
    }
}
