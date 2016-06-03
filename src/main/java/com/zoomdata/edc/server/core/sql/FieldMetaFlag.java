/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldParams;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public enum FieldMetaFlag {

    PLAYABLE, ANALYZED, RAW_DATA_ONLY, UNIQUE, GROUP_ONLY, PARTITION, NO_RAW_DATA;

    public static void addFlags(FieldMetadata metadata, FieldMetaFlag... toAdd) {
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
