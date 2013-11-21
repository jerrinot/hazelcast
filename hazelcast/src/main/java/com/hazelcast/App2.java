package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class App2 {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        //we need to set a group to prevent forming a cluster with app1
        config.getGroupConfig().setName("app2");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        IMap map = hz1.getMap("map2");
        for(int k=0;k<10000000;k++){
            map.put(k,k);
            Thread.sleep(1000);
        }
        System.out.println("Done");
    }
}