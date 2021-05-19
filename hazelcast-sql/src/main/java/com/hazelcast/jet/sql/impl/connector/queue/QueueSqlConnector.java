package com.hazelcast.jet.sql.impl.connector.queue;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class QueueSqlConnector implements SqlConnector {
    public static final String TYPE_NAME = "Queue";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Nonnull
    @Override
    public List<MappingField> resolveAndValidateFields(@Nonnull NodeEngine nodeEngine, @Nonnull Map<String, String> options, @Nonnull List<MappingField> userFields) {
        return asList(
                new MappingField("id", QueryDataType.INT),
                new MappingField("name", QueryDataType.VARCHAR)
        );
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nonnull
    @Override
    public Table createTable(@Nonnull NodeEngine nodeEngine, @Nonnull String schemaName, @Nonnull String mappingName, @Nonnull String externalName, @Nonnull Map<String, String> options, @Nonnull List<MappingField> resolvedFields) {
        return new QueueTable(schemaName, mappingName);
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(@Nonnull DAG dag, @Nonnull Table table0, @Nullable Expression<Boolean> predicate, @Nonnull List<Expression<?>> projection) {
        QueueTable table = (QueueTable) table0;
        StreamSourceTransform<Object[]> source = (StreamSourceTransform<Object[]>) table.items(predicate, projection);
        ProcessorMetaSupplier pms = source.metaSupplierFn.apply(EventTimePolicy.noEventTime());
        return dag.newUniqueVertex(table.toString(), pms);
    }

    private class QueueTable extends JetTable {
        public QueueTable(String schemaName, String mappingName) {
            super(QueueSqlConnector.this, Arrays.asList(new TableField("id", QueryDataType.INT, false),
                    new TableField("name", QueryDataType.VARCHAR, false)
            ), schemaName, mappingName, new ConstantTableStatistics(0));
        }

        @Override
        public PlanObjectKey getObjectKey() {
            //todo: figure out what does it mean
            return null;
        }

        StreamSource<Object[]> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
            return SourceBuilder.stream("foo", c -> new Object())
                    .<Object[]>fillBufferFn((c, b) -> b.add(new Object[]{0, "foo"}))
                    .build();
        }
    }
}
