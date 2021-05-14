package com.hazelcast.spring.jet;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.processor.Initializable;
import com.hazelcast.spring.context.SpringAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.function.Function;

public final class SpringBeans {

    public static <I, O> FunctionEx<I, O> beanFunction(String beanName) {
        return new FunctionDelegate<>(beanName);
    }

    public static <T> SupplierEx<T> beanSupplier(String beanName) {
        return new BeanSupplier<>(beanName, FunctionEx.identity());
    }

    public static SupplierEx<Connection> connectionSupplier(String dataSourceBeanName) {
        return new BeanSupplier<DataSource, Connection>(dataSourceBeanName, DataSource::getConnection);
    }

    @SpringAware
    private static final class BeanSupplier<B, O> implements SupplierEx<O>, Initializable {
        private final String beanName;
        private final FunctionEx<B, O> fce;
        @Autowired
        private transient ApplicationContext context;

        private BeanSupplier(String beanName, FunctionEx<B, O> fce) {
            this.beanName = beanName;
            this.fce = fce;
        }

        @Override
        public O getEx() throws Exception {
            return fce.applyEx((B)context.getBean(beanName));
        }

        @Override
        public void init(@Nonnull Processor.Context context) {
            context.managedContext().initialize(this);
        }
    }

    @SpringAware
    private static final class FunctionDelegate<I, O> implements FunctionEx<I, O>, InitializingBean, Initializable {
        private final String beanName;

        @Autowired
        private transient ApplicationContext context;
        private transient Function<I, O> delegate;

        private FunctionDelegate(String beanName) {
            this.beanName = beanName;
        }

        @Override
        public O applyEx(I i) {
            return delegate.apply(i);
        }

        @Override
        public void afterPropertiesSet() throws Exception {
            this.delegate = context.getBean(beanName, Function.class);
        }

        @Override
        public void init(Processor.Context context) {
            context.managedContext().initialize(this);
        }
    }
}
