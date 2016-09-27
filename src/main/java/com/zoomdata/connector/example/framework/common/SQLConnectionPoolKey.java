/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

@Getter
@ToString(exclude = {"password"})
@EqualsAndHashCode
public final class SQLConnectionPoolKey {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final Map<String, String> customParams;

    public SQLConnectionPoolKey(String jdbcUrl, String username, String password) {
        this(jdbcUrl, username, password, null);
    }

    public SQLConnectionPoolKey(String jdbcUrl, String username, String password, Map<String, String> customParams) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.customParams = customParams == null ? ImmutableMap.of() : ImmutableMap.copyOf(customParams);
    }
}
