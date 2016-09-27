/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcCommons {

    private static final Logger log = LoggerFactory.getLogger(JdbcCommons.class);

    public static final String DEFAULT_PARAMS_DELIMITER = "&";

    public static <T> List<T> listAndClose(PreparedStatement ps, IResultSetMapper<T> mapper)
        throws SQLException {
        return list(ps, Collections.emptyList(), mapper);
    }

    public static <T> List<T> list(PreparedStatement ps, List<DelayedParameter> delayed, IResultSetMapper<T> mapper)
        throws SQLException {

        final List<T> results = new ArrayList<>();
        // process delayed parameters for prepared statement
        for (DelayedParameter param : delayed) {
            param.apply(ps);
        }
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                results.add(mapper.map(rs));
            }
            return results;
        }
    }

    public static <T> List<T> list(ResultSet rs, IResultSetMapper<T> mapper)
        throws SQLException {
        final List<T> results = new ArrayList<>();
        while (rs.next()) {
            results.add(mapper.map(rs));
        }
        return results;
    }

    public static <T> List<T> listAndClose(ResultSet rs, IResultSetMapper<T> mapper)
        throws SQLException {
        try {
            return list(rs, mapper);
        } finally {
            JdbcCommons.closeResultSet(rs);
        }
    }

    public static <T> Set<T> set(PreparedStatement ps, IResultSetMapper<T> mapper)
        throws SQLException {
        return set(ps, Collections.emptyList(), mapper);
    }

    public static <T> Set<T> set(PreparedStatement ps, List<DelayedParameter> delayed, IResultSetMapper<T> mapper)
        throws SQLException {
        final Set<T> results = new HashSet<>();
        for (DelayedParameter param : delayed) {
            param.apply(ps);
        }
        try (final ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                results.add(mapper.map(rs));
            }
            return results;
        }
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

    public static void closeStatement(Statement statement) {
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

    public static void executeDDL(String query, Connection connection) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.executeUpdate(query);
        }
    }

    public static String prepareParameters(String jdbcUrl, Map<String, String> parameters) {
        return prepareParameters(jdbcUrl, parameters, DEFAULT_PARAMS_DELIMITER);
    }

    public static String prepareParameters(String jdbcUrl, Map<String, String> parameters, String delimiter) {
        if (StringUtils.isEmpty(jdbcUrl) || parameters.isEmpty()) {
            return jdbcUrl;
        }

        String customParameters = parameters.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(delimiter));

        int queryStartPosition = jdbcUrl.indexOf("?");
        if (queryStartPosition == -1) {
            return jdbcUrl + "?" + customParameters;
        } else {
            return jdbcUrl.substring(0, queryStartPosition)
                + "?" + customParameters + delimiter
                + jdbcUrl.substring(queryStartPosition + 1, jdbcUrl.length());
        }
    }

    public static String extractColumnTypeName(ResultSetMetaData resultSetMetaData, int index) throws SQLException {
        return resultSetMetaData.getColumnTypeName(index);
    }
}
