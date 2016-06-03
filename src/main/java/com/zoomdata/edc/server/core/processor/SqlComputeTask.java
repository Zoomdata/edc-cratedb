/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.processor;

import com.zoomdata.edc.server.core.Cursor;
import com.zoomdata.edc.server.core.JdbcCommons;
import com.zoomdata.edc.server.core.Meta;
import com.zoomdata.edc.server.cratedb.CrateDbTypesMapping;
import com.zoomdata.edc.server.core.utils.StringUtils;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.Record;
import com.zoomdata.gen.edc.types.ResponseMetadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqlComputeTask {

    private Connection connection;
    private PreparedStatement statement;

    private ResultSet resultSet;

    public SqlComputeTask(Connection connection, PreparedStatement statement) {
        this.connection = connection;
        this.statement = statement;
    }

    public Cursor compute() {
        try {
            resultSet = statement.executeQuery();

            final boolean initialHasNext = resultSet.next();
            final List<ResponseMetadata> metadataList = new ArrayList<>();
            final List<String> jdbcMetadataList = new ArrayList<>();
            populateMetadata(resultSet, metadataList, jdbcMetadataList);

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
                            Meta type = CrateDbTypesMapping.metaForType(jdbcType);
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
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void populateMetadata(ResultSet resultSet, List<ResponseMetadata> metadataList, List<String> jdbcMetadataList) {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                final ResponseMetadata thriftMetadata = new ResponseMetadata();
                final String columnType =
                        StringUtils.extractType(
                                resultSetMetaData.getColumnTypeName(i).toUpperCase());
                final String columnLabel = resultSetMetaData.getColumnLabel(i);
                thriftMetadata.setName(columnLabel);
                thriftMetadata.setType(CrateDbTypesMapping.metaForType(columnType).getThriftType());
                jdbcMetadataList.add(columnType);
                metadataList.add(thriftMetadata);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public double progress() {
        return 0;
    }

    public void cancel() {
        return;
        /* cancel not supported.
        try {
            statement.cancel();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }*/
    }

    public void close() {
        JdbcCommons.closeResultSet(resultSet);
        JdbcCommons.closeStatement(statement);
        JdbcCommons.closeConnection(connection);
    }
}