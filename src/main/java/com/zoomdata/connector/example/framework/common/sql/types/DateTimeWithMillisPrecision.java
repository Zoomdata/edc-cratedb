/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.types;

import com.querydsl.sql.types.DateTimeType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateTimeWithMillisPrecision extends DateTimeType {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat
        .forPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZoneUTC();

    @Override
    public String getLiteral(DateTime value) {
        return dateTimeFormatter.print(value);
    }

}
