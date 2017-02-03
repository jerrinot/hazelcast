package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RuntimeMetricSetTest extends HazelcastTestSupport {

    private final static int TEN_MB = 10 * 1024 * 1024;

    private MetricsRegistryImpl metricsRegistry;
    private Runtime runtime;

    @Before
    public void setup() {
        ILogger logger = mock(ILogger.class);
        HazelcastThreadGroup hazelcastThreadGroup = new HazelcastThreadGroup("name", logger, null);
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO, hazelcastThreadGroup);
        RuntimeMetricSet.register(metricsRegistry);
        runtime = Runtime.getRuntime();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(RuntimeMetricSet.class);
    }

    @Test
    public void freeMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.freeMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.freeMemory(), gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void totalMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.totalMemory");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.totalMemory(), gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void maxMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.maxMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.maxMemory(), gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void usedMemory() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.usedMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = runtime.totalMemory() - runtime.freeMemory();
                assertEquals(expected, gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void availableProcessors() {
        LongGauge gauge = metricsRegistry.newLongGauge("runtime.availableProcessors");
        assertEquals(runtime.availableProcessors(), gauge.read());
    }

    @Test
    public void uptime() {
        final LongGauge gauge = metricsRegistry.newLongGauge("runtime.uptime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = ManagementFactory.getRuntimeMXBean().getUptime();
                assertEquals(expected, gauge.read(), TimeUnit.MINUTES.toMillis(1));
            }
        });
    }
}
