/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.google.common.base.Supplier;
import com.zoomdata.connector.example.framework.api.ITypesMapping;
import com.zoomdata.connector.example.framework.async.IComputeTask;
import com.zoomdata.connector.example.framework.async.IComputeTaskFactory;
import com.zoomdata.connector.example.framework.common.sql.ParametrizedQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SqlPreparedQueryComputeTaskFactory implements IComputeTaskFactory {

    protected final Supplier<Connection> connectionSupplier;
    protected final ParametrizedQuery query;
    protected final ITypesMapping typesMapping;
    protected final int fetchSize;

    public SqlPreparedQueryComputeTaskFactory(Supplier<Connection> connectionSupplier, ParametrizedQuery query,
                                              ITypesMapping typesMapping, int fetchSize) {
        this.connectionSupplier = connectionSupplier;
        this.query = query;
        this.typesMapping = typesMapping;
        this.fetchSize = fetchSize;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public IComputeTask create() {
        Connection connection = connectionSupplier.get();
        try {
            PreparedStatement statement = connection.prepareStatement(query.getSql());
            statement.setFetchSize(fetchSize);
            query.applyParameters(statement);
            return createComputeTask(connection, statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected SqlPreparedQueryComputeTask createComputeTask(Connection connection, PreparedStatement statement) {
        return new SqlPreparedQueryComputeTask(connection, statement, typesMapping);
    }

    @Override
    public String getRawQuery() {
        return query.toString();
    }
}
