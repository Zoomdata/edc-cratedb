/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.ops;

import com.querydsl.core.types.Operator;

public enum ExtendedDateTimeOps implements Operator {
    TRUNC_MILLISECOND(Comparable.class),
    TRUNC_QUARTER(Comparable.class),
    FROM_UNIXTIME(Comparable.class),
    FROM_MILLIS(Comparable.class),
    FROM_YEARPATTERN(Comparable.class);

    private final Class<?> type;

    ExtendedDateTimeOps(Class<?> type) {
        this.type = type;
    }

    @Override
    public Class<?> getType() {
        return type;
    }
}
