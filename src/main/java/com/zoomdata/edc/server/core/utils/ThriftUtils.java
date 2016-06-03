/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.utils;

import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

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

    public static FieldType detectType(Object value) {
        if (value instanceof Number) {
            return FieldType.DOUBLE;
        }
        if (value instanceof Date) {
            return FieldType.DATE;
        }
        return FieldType.STRING;
    }

    static public String GetSqlDateFromUTC(final long utc) throws Exception {
        Date date = new Date();
        date.setTime(utc);
        return GetSqlDateFromDate(date);
    }

    public static String GetSqlDateFromDate(final Date date) throws Exception {
        if (date == null) return "";
        final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(date);
    }

    static public String GetSolrDateFromSqlDate(final String sqlDate) throws Exception {
        final Date date = GetDateFromSql(sqlDate);
        return GetSolrDateFromDateObject(date, "GMT");
    }

    static public String GetSolrDateFromDateObject(final Date inDate,
                                                   final String inTimezone) throws Exception {
        Date date = inDate;
        if (date == null) return null;

        String timezone = "EST5EDT";
        if (StringUtils.isEmpty(inTimezone) == false) {
            timezone = inTimezone;
        }

        final TimeZone tz = TimeZone.getTimeZone(timezone);
        final boolean inDs = tz.inDaylightTime(date);
        // System.err.println(date + ": inDs=" + inDs);

        if (inDs == false) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            date = cal.getTime();
        } else {
            // ignore
        }

        TimeZone UTC = TimeZone.getTimeZone("Z");
        SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df2.setTimeZone(UTC);

        return df2.format(date);
    }

    public static Date GetDateFromSql(String sqlDate) throws Exception {
        if (StringUtils.isEmpty(sqlDate) == true) return null;
        sqlDate = sqlDate.trim();
        if (sqlDate.contains(" ") == false) {
            if (sqlDate.contains("-") == true) {
            } else {
                sqlDate += " 00:00:00";
            }
        }

        final String formats[] = {
                "dd/MMM/yyyy:HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss.SSS",
                "EEE MMM dd HH:mm:ss z yyyy",
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd'T'HH:mm:ssXXX",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd",
                "yyyy-MM-dd HH:mm:ss", "MM/dd/yyyy",
        };

        Date dateString = null;
        for (int i = 0; i < formats.length; i++) {
            final String format = formats[i];
            try {
                // TimeZone UTC = TimeZone.getTimeZone("UTC");
                final SimpleDateFormat df = new SimpleDateFormat(format);
                // df.setTimeZone(UTC);

                dateString = df.parse(sqlDate);
            } catch (Exception e) {
                // swallow! Keep trying.
            }
        } // end for.

        if (dateString == null) {
            throw new Exception("*** Error ***: unable to parse sqlDate " + sqlDate);
        }

        return dateString;
    }
}
