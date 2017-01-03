/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

public interface IAliasGenerator {
    String generate(String fieldName);
    String generateForPercentiles(String fieldName);
}

