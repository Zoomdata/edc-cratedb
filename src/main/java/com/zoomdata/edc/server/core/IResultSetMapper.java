/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface IResultSetMapper<T> {
    T map(ResultSet rs) throws SQLException;
}
