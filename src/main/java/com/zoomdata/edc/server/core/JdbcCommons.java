/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JdbcCommons {

    private static final Logger log = LoggerFactory.getLogger(com.zoomdata.edc.server.core.JdbcCommons.class);

    public static <T> List<T> list(ResultSet rs, com.zoomdata.edc.server.core.IResultSetMapper<T> mapper)
            throws SQLException {
        final List<T> results = new ArrayList<>();
        while (rs.next()) {
            results.add(mapper.map(rs));
        }
        return results;
    }

    public static <T> List<T> list(PreparedStatement ps, com.zoomdata.edc.server.core.IResultSetMapper<T> mapper)
            throws SQLException {
        ResultSet rs = null;
        try {
            final List<T> results = new ArrayList<>();
            rs = ps.executeQuery();
            while (rs.next()) {
                results.add(mapper.map(rs));
            }
            return results;
        } finally {
            closeResultSet(rs);
        }
    }

    public static <T> Set<T> set(PreparedStatement ps, IResultSetMapper<T> mapper)
            throws SQLException {
        final Set<T> results = new HashSet<>();
        final ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            results.add(mapper.map(rs));
        }
        return results;
    }

    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.debug("Can't close connection");
            }
        }
    }

    public static void closeStatement(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.debug("Can't close connection");
            }
        }
    }

    public static void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.debug("Can't close connection");
            }
        }
    }
}
