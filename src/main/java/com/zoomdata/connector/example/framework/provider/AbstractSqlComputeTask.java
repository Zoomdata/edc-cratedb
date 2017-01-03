/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */

package com.zoomdata.connector.example.framework.provider;

import com.zoomdata.connector.example.common.utils.StringUtils;
import com.zoomdata.connector.example.framework.api.ITypesMapping;
import com.zoomdata.connector.example.framework.async.Cursor;
import com.zoomdata.connector.example.framework.async.IComputeTask;
import com.zoomdata.connector.example.framework.common.JdbcCommons;
import com.zoomdata.connector.example.framework.common.Meta;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.Record;
import com.zoomdata.gen.edc.types.ResponseMetadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSqlComputeTask implements IComputeTask {

    protected final Connection connection;
    protected final Statement statement;
    protected final ITypesMapping typesMapping;

    private volatile ResultSet resultSet;

    public AbstractSqlComputeTask(Connection connection, Statement statement, ITypesMapping typesMapping) {
        this.connection = connection;
        this.statement = statement;
        this.typesMapping = typesMapping;
    }

    @Override
    public Cursor compute() {
        try {
            resultSet = execute(statement);
            return toCursor(resultSet);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double progress() {
        return 0;
    }

    @Override
    public void cancel() {
        try {
            statement.cancel();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        JdbcCommons.closeResultSet(resultSet);
        JdbcCommons.closeStatement(statement);
        JdbcCommons.closeConnection(connection);
    }

    protected abstract ResultSet execute(Statement statement) throws SQLException;

    protected Cursor toCursor(ResultSet resultSet) throws SQLException {
        final List<ResponseMetadata> metadataList = new ArrayList<>();
        final List<String> jdbcMetadataList = new ArrayList<>();
        populateMetadata(resultSet, metadataList, jdbcMetadataList);

        final boolean initialHasNext = resultSet.next();

        return new Cursor() {
            private boolean hasNext = initialHasNext;

            @Override
            public List<ResponseMetadata> getMetadata() {
                return metadataList;
            }

            @Override
            public boolean hasNextBatch() {
                return false;
            }

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Record next() {
                try {
                    final Record record = new Record();
                    for (int i = 1; i <= jdbcMetadataList.size(); i++) {
                        String jdbcType = jdbcMetadataList.get(i - 1);
                        Meta type = typesMapping.metaForType(jdbcType);
                        final Field field = type.getRsFunction().apply(resultSet, i);
                        record.addToRecord(field);
                    }

                    hasNext = resultSet.next();
                    return record;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    protected void populateMetadata(ResultSet resultSet, List<ResponseMetadata> metadataList, List<String> jdbcMetadataList) {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                final ResponseMetadata thriftMetadata = new ResponseMetadata();
                final String columnType =
                    StringUtils.extractType(
                        extractColumnTypeName(resultSetMetaData, i).toUpperCase());
                final String columnLabel = extractColumnName(resultSetMetaData, i);
                thriftMetadata.setName(columnLabel);
                thriftMetadata.setType(typesMapping.metaForType(columnType).getThriftType());
                jdbcMetadataList.add(columnType);
                metadataList.add(thriftMetadata);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected String extractColumnName(ResultSetMetaData resultSetMetaData, int index) throws SQLException {
        return resultSetMetaData.getColumnLabel(index);
    }

    protected String extractColumnTypeName(ResultSetMetaData resultSetMetaData, int index) throws SQLException {
        return JdbcCommons.extractColumnTypeName(resultSetMetaData, index);
    }
}
