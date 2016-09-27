/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import com.querydsl.sql.SQLTemplates;
import com.zoomdata.connector.example.common.utils.StringUtils;
import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.request.DataSourceInfo;
import com.zoomdata.gen.edc.request.RequestInfo;

import java.util.*;
import java.util.stream.Collectors;

public final class PropertiesExtractor {
    private PropertiesExtractor() {
    }

    public static String extractJdbcURL(RequestInfo info) {
        assertParamsNotEmpty(info);
        // TODO contract for keys
        return info.getDataSourceInfo().getParams().get("JDBC_URL");
    }

    public static String extractUsername(RequestInfo info) {
        assertParamsNotEmpty(info);
        return info.getDataSourceInfo().getParams().get("USER_NAME");
    }

    public static String extractPassword(RequestInfo info) {
        assertParamsNotEmpty(info);
        return info.getDataSourceInfo().getParams().get("PASSWORD");
    }

    public static String extractKeyPath(RequestInfo info) {
        assertParamsNotEmpty(info);
        String keyPath = info.getDataSourceInfo().getParams().get("KEY_PATH");
        if (keyPath == null) {
            throw new IllegalArgumentException("Key path is required.");
        }
        return keyPath.trim();
    }

    public static List<String> extractPublicProjects(RequestInfo info) {
        assertParamsNotEmpty(info);
        String publicProjects = info.getDataSourceInfo().getParams().get("PUBLIC_PROJECT_IDS");
        if (publicProjects == null) {
            return new ArrayList<>();
        } else {
            return Arrays.asList(publicProjects.split(","))
                    .stream()
                    .map(String::trim)
                    .collect(Collectors.toList());

        }
    }

    public static String extractConnectorTypeQuitely(RequestInfo info) {
        try {
            return extractConnectorType(info);
        } catch (IllegalArgumentException e) {
            return StringUtils.EMPTY;
        }
    }

    public static Optional<String> extractServerTimeZone(RequestInfo info) {
        // A few tests and other objects try to access this before it's available
        if (info != null && info.getDataSourceInfo() != null && info.getDataSourceInfo().isSetParams() && info.getDataSourceInfo().getParams().containsKey("SERVER_TIMEZONE")) {
            return Optional.of(info.getDataSourceInfo().getParams().get("SERVER_TIMEZONE"));
        }
        return Optional.empty();
    }

    public static Optional<String> extractCustomParameter(RequestInfo info, String name) {
        assertParamsNotEmpty(info);
        if(info.getDataSourceInfo().getParams().containsKey(name)) {
            return Optional.of(info.getDataSourceInfo().getParams().get(name));
        }
        return Optional.empty();
    }

    public static String extractConnectorType(RequestInfo info) {
        DataSourceInfo dsInfo = info.getDataSourceInfo();
        if (dsInfo == null) {
            throw new IllegalArgumentException("DataSource is required!");
        } else {
            Map<String, String> params = dsInfo.getParams();
            if (params == null) {
                throw new IllegalArgumentException("DataSource.params are required");
            } else {
                String connectorType = params.get("CONNECTOR_TYPE");
                if (connectorType == null) {
                    throw new IllegalArgumentException("Connector type is required for this server");
                } else {
                    return connectorType;
                }
            }
        }
    }

    public static SQLConnectionPoolKey poolKey(RequestInfo info) {
        return new SQLConnectionPoolKey(
                extractJdbcURL(info),
                extractUsername(info),
                extractPassword(info)
        );
    }

    public static String tableName(CollectionInfo collectionInfo) {
        return tableNameQuoted(collectionInfo, "");
    }

    public static String tableName(CollectionInfo collectionInfo, SQLTemplates sqlTemplates) {
        StringBuilder builder = new StringBuilder();
        if (collectionInfo.isSetSchema()) {
            builder.append(sqlTemplates.quoteIdentifier(collectionInfo.getSchema())).append(".");
        }
        return builder.append(sqlTemplates.quoteIdentifier(collectionInfo.getCollection())).toString();
    }

    public static String tableNameQuoted(CollectionInfo collectionInfo, String quoteString) {
        // schemas is available
        if (collectionInfo == null) {
            throw new IllegalArgumentException("Collection is required");
        } else if (collectionInfo.getSchema() != null && collectionInfo.getSchema().length() > 0) {
            return collectionInfo.getSchema() + "." + quoteString + collectionInfo.getCollection() + quoteString;
        } else {
            return collectionInfo.getCollection();
        }
    }

    private static void assertParamsNotEmpty(RequestInfo info) {
        if (info == null) {
            throw new IllegalArgumentException("Params is empty");
        }
        assertParamsNotEmpty(info.getDataSourceInfo());
    }

    private static void assertParamsNotEmpty(DataSourceInfo info) {
        if (info == null) {
            throw new IllegalArgumentException("Params is empty");
        }
    }
}
