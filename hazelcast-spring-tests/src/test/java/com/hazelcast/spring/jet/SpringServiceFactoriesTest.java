/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.jet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.processor.Initializable;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.Assertions;
import com.hazelcast.jet.pipeline.test.SimpleEvent;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.spring.CustomSpringJUnit4ClassRunner;
import com.hazelcast.spring.context.SpringAware;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.jdbc.Sql;

import javax.annotation.Nonnull;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertAnyOrder;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollected;
import static com.hazelcast.jet.pipeline.test.AssertionSinks.assertCollectedEventually;
import static com.hazelcast.spring.jet.JetSpringServiceFactories.bean;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"application-context-jet-service.xml"})
public class SpringServiceFactoriesTest {

    @Resource(name = "jet")
    private JetInstance jetInstance;

    @BeforeClass
    @AfterClass
    public static void start() {
        Hazelcast.shutdownAll();
    }

    @Resource
    private DataSource dataSource;

    @Test
    public void testMapBatchUsingSpringBean() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1L, 2L, 3L, 4L, 5L, 6L))
                .mapUsingService(bean("calculator"), Calculator::multiply)
                .writeTo(assertAnyOrder(asList(-1L, -2L, -3L, -4L, -5L, -6L)));

        jetInstance.newJob(pipeline).join();
    }

    @Test
    public void testFilterBatchUsingSpringBean() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(1L, 2L, 3L, 4L, 5L, 6L))
                .filterUsingService(bean("calculator"), Calculator::filter)
                .writeTo(assertAnyOrder(asList(2L, 4L, 6L)));

        jetInstance.newJob(pipeline).join();
    }

    @Test
    public void testMapStreamUsingSpringBean() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withNativeTimestamps(0)
                .map(SimpleEvent::sequence)
                .mapUsingService(bean("calculator"), Calculator::multiply)
                .writeTo(assertCollectedEventually(10, c -> {
                    assertTrue(c.size() > 100);
                    c.forEach(i -> assertTrue(i <= 0));
                }));

        Job job = jetInstance.newJob(pipeline);
        assertJobCompleted(job);
    }

    @Test
    public void foo() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(1))
                .withNativeTimestamps(5000)
                .window(WindowDefinition.sliding(10_000, 1000))
                .aggregate(AggregateOperations.pickAny())
                .writeTo(Sinks.logger());
        jetInstance.newJob(pipeline).join();
    }

    @Test
    @Sql("populate_h2.sql")
    public void testMapStreamUsingSpringBean_injectedToMapper() throws SQLException {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.jdbc(
                    SpringBeans.connectionSupplier("dataSourceBean"),
                    (conn, parallelism, index) -> conn.prepareStatement("select * from person").executeQuery(),
                    rs -> rs.getInt("item")))
//                .map(new AuditingFunction<>("auditTopic"))
                .filterStateful(new SpringAwareFilterSupplier(), PredicateEx::test)
                .map(SpringBeans.<Integer, Long>beanFunction("managedFunction"))
                .writeTo(assertCollected(c -> {
                    assertEquals(6, c.size());
                    c.forEach(l -> assertTrue(l < 0));
                }));
        pipeline.setPreserveOrder(true);

        AtomicInteger counter = new AtomicInteger();
        jetInstance.newJob(pipeline).join();
    }

    public static class AuditingFunction<T> implements FunctionEx<T, T>, HazelcastInstanceAware {
        private transient HazelcastInstance instance;
        private final String topicName;

        public AuditingFunction(String topicName) {
            this.topicName = topicName;
        }

        @Override
        public T applyEx(T t) {
            instance.getTopic(topicName).publish(t);
            return t;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    @SpringAware
    public static class SpringAwareFilterSupplier implements SupplierEx<PredicateEx<Integer>>  {

        @Value("${job.filter.threshold}")
        private int threshold;

        @Override
        public PredicateEx<Integer> getEx() {
            return new StatefulFilter(threshold);
        }
    }

    private static class StatefulFilter implements PredicateEx<Integer> {
        public int threshold;

        private StatefulFilter(int threshold) {
            this.threshold = threshold;
        }


        @Override
        public boolean testEx(Integer item) {
            if (item == null || threshold > item) {
                return false;
            }
            threshold = item;
            return true;
        }
    }

    @SpringAware
    public static class MapperWithSpringDeps implements FunctionEx<Integer, Long> {
        @Autowired
        private transient Calculator calculator;

        @Override
        public Long applyEx(Integer event) {
            return calculator.multiply(event);
        }

    }

    @Test
    public void testFilterStreamUsingSpringBean() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(100))
                .withNativeTimestamps(0)
                .map(SimpleEvent::sequence)
                .filterUsingService(bean("calculator"), Calculator::filter)
                .writeTo(assertCollectedEventually(10, c -> {
                    assertTrue(c.size() > 100);
                    c.forEach(i -> assertEquals(0, i % 2));
                }));

        Job job = jetInstance.newJob(pipeline);
        assertJobCompleted(job);
    }

    private static void assertJobCompleted(Job job) {
        try {
            job.join();
            fail("expected CompletionException");
        } catch (CompletionException e) {
            assertTrue(e.getMessage().contains("AssertionCompletedException: Assertion passed successfully"));
        }
    }
}
