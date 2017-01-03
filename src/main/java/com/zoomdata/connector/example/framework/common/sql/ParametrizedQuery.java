/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.google.common.collect.ImmutableList;
import lombok.ToString;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Wraps SQL query and its parameters.
 */
@ToString
public class ParametrizedQuery {
    private String sql;
    private ImmutableList<Object> parameters;

    public ParametrizedQuery(String sql) {
        this.sql = sql;
    }

    public ParametrizedQuery(String sql, ImmutableList<Object> parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    public String getSql() {
        return sql;
    }

    public ImmutableList<Object> getParameters() {
        return parameters;
    }

    public void applyParameters(PreparedStatement ps) throws SQLException {
        if (parameters != null) {
            for (int i = 0; i < parameters.size(); i++) {
                Object parameter = parameters.get(i);
                ps.setObject(i + 1, parameter);
            }
        }
    }
}
