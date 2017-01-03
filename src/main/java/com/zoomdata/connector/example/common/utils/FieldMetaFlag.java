/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldParams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public enum FieldMetaFlag {

    PLAYABLE, RAW_DATA_ONLY, PARTITION;

    public static void addFlags(FieldMetadata metadata, FieldMetaFlag... toAdd) {
        if (toAdd == null || toAdd.length == 0) {
            return;
        }

        FieldParams params = metadata.getFieldParams();
        if (params == null) {
            metadata.setFieldParams(params = new FieldParams());
        }

        List<String> fieldFlags = params.getFlags();
        if (fieldFlags == null) {
            params.setFlags(fieldFlags = new ArrayList<>());
        }

        Stream.of(toAdd).map(FieldMetaFlag::toString).forEach(fieldFlags::add);
    }
}
