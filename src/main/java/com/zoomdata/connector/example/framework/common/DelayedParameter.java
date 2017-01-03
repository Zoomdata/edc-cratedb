/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface DelayedParameter {
    void apply(PreparedStatement ps) throws SQLException;
}
