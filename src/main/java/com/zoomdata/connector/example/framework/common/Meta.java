/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common;

import com.zoomdata.gen.edc.types.FieldType;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Meta {
    private FieldType thriftType;
    private ThriftTypeFunction rsFunction;

    public static Meta from(FieldType thriftType, ThriftTypeFunction rsFunction) {
        return new Meta(thriftType, rsFunction);
    }
}
