/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.google.common.base.Supplier;
import com.zoomdata.connector.example.framework.api.ITypesMapping;
import com.zoomdata.connector.example.framework.async.IComputeTask;
import com.zoomdata.connector.example.framework.async.IComputeTaskFactory;
import com.zoomdata.connector.example.framework.common.sql.ParametrizedQuery;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlQueryComputeTaskFactory implements IComputeTaskFactory {

    protected final Supplier<Connection> connectionSupplier;
    protected final String query;
    protected final ITypesMapping typesMapping;
    protected final int fetchSize;

    public SqlQueryComputeTaskFactory(Supplier<Connection> connectionSupplier, ParametrizedQuery query,
                                      ITypesMapping typesMapping, int fetchSize) {
        if (!CollectionUtils.isEmpty(query.getParameters())) {
            throw new IllegalArgumentException("Use SqlComputeTaskFactory for parametrised query.");
        }

        this.connectionSupplier = connectionSupplier;
        this.query = query.getSql();
        this.typesMapping = typesMapping;
        this.fetchSize = fetchSize;
    }

    @Override
    public IComputeTask create() {
        Connection connection = connectionSupplier.get();
        try {
            Statement statement = connection.createStatement();
            statement.setFetchSize(fetchSize);
            return createComputeTask(connection, statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected SqlQueryComputeTask createComputeTask(Connection connection, Statement statement) {
        return new SqlQueryComputeTask(connection, statement, typesMapping, query);
    }

    @Override
    public String getRawQuery() {
        return query;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }
}
