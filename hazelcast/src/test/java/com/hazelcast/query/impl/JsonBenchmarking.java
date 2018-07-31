package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.json.Json;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class JsonBenchmarking extends HazelcastTestSupport  {

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public String serialization;

    @Parameterized.Parameters(name = "in-memory-format: {0}, serialization-strategy: {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY, "JSON"},
//                {InMemoryFormat.OBJECT, "JSON"},

//                {InMemoryFormat.BINARY, "DS"},
//                {InMemoryFormat.OBJECT, "DS"},
        });
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testPerf_json() {
        Config config = new Config();
        String mapName = "map";
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Object> map = instance.getMap(mapName);

        for (int i = 0; i < 1000000; i++) {
            Object value = newValue(i);
            map.put(i, value);
        }

        Predicate predicate = Predicates.equal("name", "non-sancar");
        for (int i = 0; i < 1000000; i++) {
            long startTime = System.nanoTime();
            Collection<?> values = map.values(predicate);
            long duration = System.nanoTime() - startTime;
            System.out.println("Took " + TimeUnit.NANOSECONDS.toMicros(duration) + " micros");
            assertEquals(0, values.size());
        }
    }

    private Object newValue(int i) {
        if ("JSON".equals(serialization)) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            return Json.parse(jsonString);
        } else if ("DS".equals(serialization)) {
            return new Person("sancar", i, (i % 2 == 0));
        } else {
            throw new UnsupportedOperationException("unknown serialization strategy: " + serialization);
        }
    }

    public static class Person implements DataSerializable {
        private String name;
        private int age;
        private boolean active;

        public Person() {

        }

        public Person(String name, int age, boolean active) {
            this.name = name;
            this.age = age;
            this.active = active;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            out.writeInt(age);
            out.writeBoolean(active);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
            age = in.readInt();
            active = in.readBoolean();
        }
    }

}
