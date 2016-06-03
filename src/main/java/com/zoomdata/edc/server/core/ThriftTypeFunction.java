/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core;

import com.zoomdata.gen.edc.types.Field;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.sql.ResultSet;
import java.sql.SQLException;

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
    com.zoomdata.edc.server.core.ThriftTypeFunction GET_INTEGER = new com.zoomdata.edc.server.core.ThriftTypeFunction() {
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
    com.zoomdata.edc.server.core.ThriftTypeFunction GET_BOOLEAN = new com.zoomdata.edc.server.core.ThriftTypeFunction() {
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
    com.zoomdata.edc.server.core.ThriftTypeFunction GET_DOUBLE = new com.zoomdata.edc.server.core.ThriftTypeFunction() {
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
    com.zoomdata.edc.server.core.ThriftTypeFunction GET_DATE = new com.zoomdata.edc.server.core.ThriftTypeFunction() {
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
    com.zoomdata.edc.server.core.ThriftTypeFunction GET_TIMESTAMP = new com.zoomdata.edc.server.core.ThriftTypeFunction() {
        @Override
        public Field apply(ResultSet rs, Integer idx) throws SQLException {
            if (rs.getObject(idx) == null) {
                return new Field().setIsNull(true);
            } else {
                // TODO encode
                return new Field().setValue(DATE_TIME_FORMATTER.print(rs.getTimestamp(idx).getTime()));
            }
        }
    };
}
