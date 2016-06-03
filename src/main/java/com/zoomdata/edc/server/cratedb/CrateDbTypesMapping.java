/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.cratedb;

import com.zoomdata.edc.server.core.Meta;
import com.zoomdata.edc.server.core.ThriftTypeFunction;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.HashMap;
import java.util.Map;

public final class CrateDbTypesMapping {

    public static Meta defaultMeta = metaString();

    public static Map<String, Meta> map = new HashMap<String, Meta>();
    static {
        map.put("BIGINT", metaInt());
        map.put("INT", metaInt());
        map.put("INT8", metaInt());
        map.put("INTEGER", metaInt());
        map.put("SERIAL", metaInt());
        map.put("SMALLINT", metaInt());
        map.put("TINYINT", metaInt());
        map.put("YEAR", metaInt());

        map.put("DEC", metaDouble());
        map.put("DECIMAL", metaDouble());
        map.put("NUMERIC", metaDouble());
        map.put("NUMBER", metaDouble());
        map.put("DOUBLE", metaDouble());
        map.put("DOUBLE PRECISION", metaDouble());
        map.put("FLOAT", metaDouble());
        map.put("FLOAT 8", metaDouble());
        map.put("REAL", metaDouble());

        map.put("CHAR", metaString());
        map.put("NATIONAL CHAR", metaString());
        map.put("VARCHAR", metaString());
        map.put("LONG VARCHAR", metaString());
        map.put("NATIONAL VARCHAR", metaString());
        map.put("BINARY", metaString());
        map.put("TINYBLOB", metaString());
        map.put("TINYTEXT", metaString());
        map.put("BLOB", metaString());
        map.put("MEDIUMBLOB", metaString());
        map.put("MEDIUMTEXT", metaString());
        map.put("LONGBLOB", metaString());
        map.put("LONGTEXT", metaString());
        map.put("ENUM", metaString());
        map.put("SET", metaString());

        map.put("DATE", metaDate());
        map.put("DATETIME", metaTimestamp());
        map.put("TIMESTAMP", metaTimestamp());
        map.put("SMALLDATETIME", metaTimestamp());
        map.put("TIME", metaTimestamp());
        map.put("TIME WITH TIME ZONE", metaTimestamp());
        map.put("TIME WITHOUT TIME ZONE", metaTimestamp());
        map.put("TIMESTAMP WITH TIME ZONE", metaTimestamp());
        map.put("TIMESTAMP WITHOUT TIME ZONE", metaTimestamp());

        map.put("BIT", metaString());
        map.put("BYTE", metaString());
        map.put("RAW", metaString());
        map.put("VARBINARY", metaString());
        map.put("INTERVAL", metaString());
        map.put("LONG VARBINARY", metaString());
        map.put("BOOL", metaString());
        map.put("BOOLEAN", metaString());
    }

    public static Meta metaForType(String type) {
        return map.getOrDefault(type, defaultMeta);
    }

    public static Meta metaInt() {
        return Meta.from(FieldType.INTEGER, ThriftTypeFunction.GET_INTEGER);
    }

    public static Meta metaDouble() {
        return Meta.from(FieldType.DOUBLE, ThriftTypeFunction.GET_DOUBLE);
    }

    public static Meta metaString() {
        return Meta.from(FieldType.STRING, ThriftTypeFunction.GET_STRING);
    }

    public static Meta metaDate() {
        return Meta.from(FieldType.DATE, ThriftTypeFunction.GET_DATE);
    }

    public static Meta metaTimestamp() {
        return Meta.from(FieldType.DATE, ThriftTypeFunction.GET_TIMESTAMP);
    }

}