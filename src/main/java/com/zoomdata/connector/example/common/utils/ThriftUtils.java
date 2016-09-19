/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.types.Field;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Collections;

public final class ThriftUtils {
    public ThriftUtils() { }

    private static DateTimeFormatter DATE_TIME_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    public static Long getInteger(Field value) {
        return value.isIsNull() ? null : Long.parseLong(value.getValue());
    }

    public static Double getDouble(Field value) {
        return value.isIsNull() ? null : Double.parseDouble(value.getValue());
    }

    public static String getString(Field value) {
        return value.isIsNull() ? null : value.getValue();
    }

    public static DateTime getDateTime(Field value) {
        return value.isIsNull() ? null : DATE_TIME_FORMATTER.parseDateTime(value.getValue());
    }

    public static CollectionInfo getCollectionInfo(String schema, String tableName) {
        CollectionInfo ci = new CollectionInfo();
        ci.setSchema(schema);
        ci.setCollection(tableName);
        ci.setParams(Collections.emptyMap());
        return ci;
    }
}
