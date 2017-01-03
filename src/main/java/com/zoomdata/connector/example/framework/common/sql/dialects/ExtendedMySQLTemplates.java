/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.dialects;

import com.querydsl.core.types.Ops;
import com.querydsl.sql.MySQLTemplates;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.connector.example.framework.common.sql.ops.ExtendedDateTimeOps;

/**
 * Extended template for MySQL. QueryDSL not supporting operations TRUNC_MILLISECOND and TRUNC_QUARTER required for time
 * grouping. This template implements them.

 */
public class ExtendedMySQLTemplates extends MySQLTemplates {
    public ExtendedMySQLTemplates() {
        this('\\', false);
    }

    public ExtendedMySQLTemplates(boolean quote) {
        this('\\', quote);
    }

    public ExtendedMySQLTemplates(char escape, boolean quote) {
        super(escape, quote);

        // MySQL has no milliseconds - just microseconds. Using them, taking just first 3 digits.
        add(ExtendedDateTimeOps.TRUNC_MILLISECOND,
                "str_to_date(substring(date_format({0},'%Y-%m-%d %k:%i:%s.%f'), 1, 23),'%Y-%m-%d %k:%i:%s.%f')");

        // MySQL 5.7 still has no quarter in date_format (there is a feature request for this).
        // Using quarter() function instead. (quarter(ds.hire_date) - 1) * 3 is the number of the first month of quarter.
        add(ExtendedDateTimeOps.TRUNC_QUARTER,
                "str_to_date(concat(date_format({0},'%Y-'), (quarter({0}) - 1) * 3, '-1'),'%Y-%m-%d')");

        add(Ops.DateTimeOps.TRUNC_WEEK,   "str_to_date(date_format({0},'%Y-%U-0'),'%Y-%U-%w')");

        add(ExtendedDateTimeOps.FROM_UNIXTIME, "FROM_UNIXTIME({0})");
        add(ExtendedDateTimeOps.FROM_MILLIS, "FROM_UNIXTIME({0} / 1000)");
        add(ExtendedDateTimeOps.FROM_YEARPATTERN, "STR_TO_DATE(concat({0}, '-1-1'), '%Y-%m-%d')");
    }

    public static Builder builder() {
        return new Builder() {
            @Override
            protected SQLTemplates build(char escape, boolean quote) {
                return new ExtendedMySQLTemplates(escape, quote);
            }
        };
    }
}
