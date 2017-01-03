/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.zoomdata.connector.example.framework.api.ITypesMapping;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlQueryComputeTask extends AbstractSqlComputeTask {

    private final String query;

    public SqlQueryComputeTask(Connection connection, Statement statement, ITypesMapping typesMapping, String query) {
        super(connection, statement, typesMapping);
        this.query = query;
    }

    @Override
    protected ResultSet execute(Statement statement) throws SQLException {
        return statement.executeQuery(query);
    }
}
