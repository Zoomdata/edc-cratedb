/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface IResultSetMapper<T> {
    T map(ResultSet rs) throws SQLException;
}
