/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.sql.MySQLTemplates;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps;

public class ExtendedMySQLTemplates extends MySQLTemplates {

    public ExtendedMySQLTemplates() {
        super('\\', true);
        setPrintSchema(true);

        // MySQL has no milliseconds - just microseconds. Using them, taking just first 3 digits.
        add(com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps.TRUNC_MILLISECOND,
                "str_to_date(substring(date_format({0},'%Y-%m-%d %k:%i:%s.%f'), 1, 23),'%Y-%m-%d %k:%i:%s.%f')");

        // MySQL 5.7 still has no quarter in date_format (there is a feature request for this).
        // Using quarter() function instead. (quarter(ds.hire_date) - 1) * 3 is the number of the first month of quarter.
        add(com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps.TRUNC_QUARTER,
                "str_to_date(concat(date_format({0},'%Y-'), (quarter({0}) - 1) * 3, '-1'),'%Y-%m-%d')");

        add(com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps.FROM_UNIXTIME, "FROM_UNIXTIME({0})");
        add(com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps.FROM_MILLIS, "FROM_UNIXTIME({0} / 1000)");
        add(com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps.FROM_YEARPATTERN, "STR_TO_DATE(concat({0}, '-1-1'), '%Y-%m-%d')");
    }
}
