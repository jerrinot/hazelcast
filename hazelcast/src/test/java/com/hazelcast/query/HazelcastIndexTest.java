package com.hazelcast.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.Serializable;

public class HazelcastIndexTest {
    public static void main( String[] args )
    {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("test");
//        mapConfig.addMapIndexConfig( new MapIndexConfig( "name", false ) );
        mapConfig.addMapIndexConfig( new MapIndexConfig( "testInterface.foo", false ) );
        config.addMapConfig( mapConfig );
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        IMap<Object, Object> testMap = hazelcastInstance.getMap("test");

        Person person = new Person();
        person.setTestInterface(new TestInterfaceImpl());
        testMap.put("key", person);

        hazelcastInstance.shutdown();
    }

    private static class Person implements Serializable
    {
        private String m_name;
        private TestInterface m_testInterface;

        public String getName()
        {
            return m_name;
        }

        public void setName( String name )
        {
            m_name = name;
        }

        public TestInterface getTestInterface()
        {
            return m_testInterface;
        }

        public void setTestInterface( TestInterface testInterface )
        {
            m_testInterface = testInterface;
        }
    }

    private interface TestInterface
    {
//        String getFoo();
    }

    private static class TestInterfaceImpl implements TestInterface, Serializable
    {
        private String m_foo;

        public String getFoo()
        {
            return m_foo;
        }

        public void setFoo( String foo )
        {
            m_foo = foo;
        }
    }
}
