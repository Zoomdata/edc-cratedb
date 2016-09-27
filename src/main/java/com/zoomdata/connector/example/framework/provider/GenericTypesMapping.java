/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.google.common.collect.ImmutableMap;
import com.zoomdata.connector.example.framework.api.ITypesMapping;
import com.zoomdata.connector.example.framework.common.Meta;
import com.zoomdata.connector.example.framework.common.ThriftTypeFunction;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.Map;

@SuppressWarnings("checkstyle:visibilitymodifier")
public class GenericTypesMapping implements ITypesMapping {

    protected Meta defaultMeta = metaString();

    protected Map<String, Meta> typesMapping =
            ImmutableMap.<String, Meta>builder()
                    // integer types
                    .put("BIGINT UNSIGNED", metaInt())
                    .put("INT UNSIGNED", metaInt())
                    .put("SMALLINT UNSIGNED", metaInt())
                    .put("INTEGER UNSIGNED", metaInt())
                    .put("TINYINT UNSIGNED", metaInt())

                    .put("BIGINT", metaInt())
                    .put("INT", metaInt())
                    .put("INT8", metaInt())
                    .put("INTEGER", metaInt())
                    .put("SERIAL", metaInt())
                    .put("SMALLINT", metaInt())
                    .put("TINYINT", metaInt())
                    .put("LONG", metaInt())

                    .put("YEAR", metaInt())

                            // decimal types
                    .put("DEC", metaDouble())
                    .put("MONEY", metaDouble())
                    .put("DECIMAL", metaDouble())
                    .put("NUMERIC", metaDouble())
                    .put("NUMBER", metaDouble())
                    .put("DOUBLE", metaDouble())
                    .put("DOUBLE PRECISION", metaDouble())
                    .put("FLOAT", metaDouble())
                    .put("FLOAT 8", metaDouble())
                    .put("REAL", metaDouble())

                            // string types
                    .put("CHAR", metaString())
                    .put("NATIONAL CHAR", metaString())
                    .put("VARCHAR", metaString())
                    .put("LONG VARCHAR", metaString())
                    .put("NATIONAL VARCHAR", metaString())
                    .put("BINARY", metaString())
                    .put("TINYBLOB", metaString())
                    .put("TINYTEXT", metaString())
                    .put("BLOB", metaString()) // TODO [oleksandr.chornyi]: Check that mapping of exotic types is correct
                    .put("MEDIUMBLOB", metaString())
                    .put("MEDIUMTEXT", metaString())
                    .put("LONGBLOB", metaString())
                    .put("LONGTEXT", metaString())
                    .put("TEXT", metaString())
                    .put("ENUM", metaString())
                    .put("SET", metaString())
                    .put("LIST", metaString())

                            // date types
                    .put("DATE", metaDate())
                    .put("DATETIME", metaTimestamp())
                    .put("TIMESTAMP", metaTimestamp())
                    .put("SMALLDATETIME", metaTimestamp())
                    .put("TIME", metaTime())
                    .put("TIME WITH TIME ZONE", metaTime())
                    .put("TIME WITHOUT TIME ZONE", metaTime())
                    .put("TIMESTAMP WITH TIME ZONE", metaTimestamp())
                    .put("TIMESTAMP WITHOUT TIME ZONE", metaTimestamp())

                     // unsupported types maps to string by default
                    .put("BIT", metaString())
                    .put("BYTE", metaString())
                    .put("RAW", metaString())
                    .put("VARBINARY", metaString())
                    .put("INTERVAL", metaString())
                    .put("LONG VARBINARY", metaString())
                    .put("BOOL", metaString())
                    .put("BOOLEAN", metaString())
            .build();

    @Override
    public Meta metaForType(String type) {
        return typesMapping.getOrDefault(type, defaultMeta);
    }

    protected static Meta metaInt() {
        return Meta.from(FieldType.INTEGER, ThriftTypeFunction.GET_INTEGER);
    }

    protected static Meta metaUnknown() {
        return Meta.from(FieldType.UNKNOWN, ThriftTypeFunction.GET_UNKNOWN);
    }

    protected static Meta metaDouble() {
        return Meta.from(FieldType.DOUBLE, ThriftTypeFunction.GET_DOUBLE);
    }

    protected static Meta metaString() {
        return Meta.from(FieldType.STRING, ThriftTypeFunction.GET_STRING);
    }

    protected static Meta metaDate() {
        return Meta.from(FieldType.DATE, ThriftTypeFunction.GET_DATE);
    }

    protected static Meta metaTimestamp() {
        return Meta.from(FieldType.DATE, ThriftTypeFunction.GET_TIMESTAMP);
    }

    protected Meta metaTime() {
        return Meta.from(FieldType.STRING, ThriftTypeFunction.GET_TIME_AS_STRING);
    }
}
