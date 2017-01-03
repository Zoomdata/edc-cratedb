/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import com.zoomdata.gen.edc.types.Field;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;

public interface ThriftTypeFunction {
    Field apply(ResultSet rs, Integer idx) throws SQLException;

    DateTimeFormatter DATE_TIME_FORMATTER = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);

    // retrieve string value
    ThriftTypeFunction GET_STRING = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO encode
                return new Field().setValue(rs.getString(idx));
            }
        }
    };

    // retrieve integer values
    ThriftTypeFunction GET_INTEGER = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO endcode
                return new Field().setValue("" + rs.getLong(idx));
            }
        }
    };

    // retrieve integer values
    ThriftTypeFunction GET_BOOLEAN = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO encode
                return new Field().setValue(rs.getString(idx));
            }
        }
    };

    // retrieve integer values
    ThriftTypeFunction GET_DOUBLE = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO encode
                return new Field().setValue("" + rs.getDouble(idx));
            }
        }
    };

    // retrieve integer values
    ThriftTypeFunction GET_DATE = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO encode
                return new Field().setValue(DATE_TIME_FORMATTER.print(rs.getDate(idx).getTime()));
            }
        }
    };

    // retrieve integer values
    ThriftTypeFunction GET_TIMESTAMP = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO encode
                long epochMilli = rs.getTimestamp(idx).toLocalDateTime().toInstant(ZoneOffset.UTC).toEpochMilli();
                return new Field().setValue(DATE_TIME_FORMATTER.print(epochMilli));
            }
        }
    };

    // retrieve unknown values as null
    ThriftTypeFunction GET_UNKNOWN = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
                return new Field().setIsNull(true);
        }
    };

    ThriftTypeFunction GET_TIME_AS_STRING = new ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                return new Field().setValue(rs.getTime(idx).toString());
            }
        }
    };
}
