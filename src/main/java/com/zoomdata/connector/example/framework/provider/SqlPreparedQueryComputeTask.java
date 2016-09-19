/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.zoomdata.connector.example.framework.api.ITypesMapping;

import java.sql.*;

public class SqlPreparedQueryComputeTask extends AbstractSqlComputeTask {

    public SqlPreparedQueryComputeTask(Connection connection, PreparedStatement statement, ITypesMapping typesMapping) {
        super(connection, statement, typesMapping);
    }

    @Override
    protected ResultSet execute(Statement statement) throws SQLException {
        return ((PreparedStatement) statement).executeQuery();
    }
}
