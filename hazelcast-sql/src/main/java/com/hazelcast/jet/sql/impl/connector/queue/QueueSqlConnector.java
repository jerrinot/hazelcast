package com.hazelcast.jet.sql.impl.connector.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.RowProjector;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.extract.JsonQueryTarget;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryTarget;
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
    public Table createTable(@Nonnull NodeEngine nodeEngine, @Nonnull String schemaName, @Nonnull String mappingName, @Nonnull String externalName, @Nonnull Map<String, String> options, @Nonnull List<MappingField> resolvedFields) {
        return new QueueTable(schemaName, mappingName, externalName);
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
        private final String queueName;

        public QueueTable(String schemaName, String mappingName, String queueName) {
            super(QueueSqlConnector.this, Arrays.asList(new TableField("id", QueryDataType.INT, false),
                    new TableField("name", QueryDataType.VARCHAR, false)
            ), schemaName, mappingName, new ConstantTableStatistics(0));
            this.queueName = queueName;
        }

        @Override
        public PlanObjectKey getObjectKey() {
            //todo: figure out what does it mean
            return null;
        }

        StreamSource<Object[]> items(Expression<Boolean> predicate, List<Expression<?>> projections) {
            return SourceBuilder.stream("foo", c -> c.jetInstance().getHazelcastInstance().getQueue(queueName))
                    .<Object[]>fillBufferFn((c, b) -> b.add(c.poll()))
                    .build();
        }

        QueryDataType[] types() {
            return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
        }
    }

    private static final class QueueContext {
        private IQueue queue;
        private RowProjector projector;

        private static QueueContext fromProcContext(Processor.Context procContext, String queueName, QueueTable queueTable, Expression<Boolean> predicate, List<Expression<?>> projections) {
            QueueContext queueContext = new QueueContext();
            queueContext.queue = procContext.jetInstance().getHazelcastInstance().getQueue(queueName);
            SimpleExpressionEvalContext evalCtx = SimpleExpressionEvalContext.from(procContext);

            String[] paths = {"id"};
            QueryDataType[] types = queueTable.types();
            QueryTarget queryTarget = null;
            queueContext.projector = new RowProjector(paths, types, queryTarget, predicate, projections, evalCtx);
        }

        private Object[] pollAndProject() {
            Object o = queue.poll();
            if (o == null) {
                return null;
            }
            return projector.project(o);
        }
    }
}
