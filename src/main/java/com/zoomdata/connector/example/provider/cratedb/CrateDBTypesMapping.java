/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb;

import com.google.common.collect.ImmutableMap;
import com.zoomdata.connector.example.framework.api.ITypesMapping;
import com.zoomdata.connector.example.framework.common.Meta;
import com.zoomdata.connector.example.framework.common.ThriftTypeFunction;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.Map;

// Create a mapping of Crate types to Zoomdata types
// From https://crate.io/docs/reference/sql/data_types.html
public class CrateDBTypesMapping implements ITypesMapping {

    private final Meta defaultMeta = metaString();

    private final Map<String, Meta> typesMapping =
        ImmutableMap.<String, Meta>builder()
            // Primitives
            .put("boolean", metaString())
            .put("byte", metaInt())
            .put("short", metaInt())
            .put("integer", metaInt())
            .put("long", metaInt())
            .put("float", metaDouble())
            .put("double", metaDouble())
            .put("string", metaString())
            .put("ip", metaString())
            .put("timestamp", metaTimestamp())
            // Geo types - Zoomdata generally doesn't handle native geo types for now
            // You can flatten geo types to a lat and long field and provide that way
            .put("geo_point", metaUnknown())
            .put("geo_shape", metaUnknown())
            // Compound Types - Zoomdata typically needs nested or complex types to be flattened
            // or specially handled
            .put("object", metaUnknown())
            .put("array", metaUnknown())
            .build();

    private static Meta metaInt() {
        return Meta.from(FieldType.INTEGER, ThriftTypeFunction.GET_INTEGER);
    }

    private static Meta metaUnknown() {
        return Meta.from(FieldType.UNKNOWN, ThriftTypeFunction.GET_UNKNOWN);
    }

    private static Meta metaDouble() {
        return Meta.from(FieldType.DOUBLE, ThriftTypeFunction.GET_DOUBLE);
    }

    private static Meta metaString() {
        return Meta.from(FieldType.STRING, ThriftTypeFunction.GET_STRING);
    }

    private static Meta metaDate() {
        return Meta.from(FieldType.DATE, ThriftTypeFunction.GET_DATE);
    }

    private static Meta metaTimestamp() {
        return Meta.from(FieldType.DATE, ThriftTypeFunction.GET_TIMESTAMP);
    }

    private Meta metaTime() {
        return Meta.from(FieldType.STRING, ThriftTypeFunction.GET_TIME_AS_STRING);
    }

    @Override
    public Meta metaForType(String type) {
        return typesMapping.getOrDefault(type.toLowerCase(), defaultMeta);
    }
}
