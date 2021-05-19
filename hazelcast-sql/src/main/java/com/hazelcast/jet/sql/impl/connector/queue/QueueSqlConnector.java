package com.hazelcast.jet.sql.impl.connector.queue;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
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
        return new JetTable(this,
                asList(new TableField("id", QueryDataType.INT, false),
                        new TableField("name", QueryDataType.VARCHAR, false)
                ), schemaName, mappingName, new ConstantTableStatistics(0)) {
            @Override
            public PlanObjectKey getObjectKey() {
                //todo: figure out what does it mean
                return null;
            }
        };
    }

    @Nonnull
    @Override
    public Vertex fullScanReader(@Nonnull DAG dag, @Nonnull Table table, @Nullable Expression<Boolean> predicate, @Nonnull List<Expression<?>> projection) {
        return SqlConnector.super.fullScanReader(dag, table, predicate, projection);
    }
}
