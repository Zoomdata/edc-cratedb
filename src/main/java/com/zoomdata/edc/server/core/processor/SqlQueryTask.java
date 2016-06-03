/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.processor;

import com.zoomdata.edc.server.core.ParametrizedQuery;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Supplier;

public class SqlQueryTask {

    private Supplier<Connection> connectionSupplier;
    private ParametrizedQuery query;
    private int fetchSize;

    public SqlQueryTask(Supplier<Connection> connectionSupplier, ParametrizedQuery query, int fetchSize) {
        this.connectionSupplier = connectionSupplier;
        this.query = query;
        this.fetchSize = fetchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public SqlComputeTask execute() {
        try {
            Connection connection = connectionSupplier.get();
            PreparedStatement statement = connection.prepareStatement(query.getSql());
        //    statement.setFetchSize(fetchSize);
            query.applyParameters(statement);
            return new SqlComputeTask(connection, statement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}