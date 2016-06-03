/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core;

import com.zoomdata.gen.edc.types.FieldType;

public class Meta {
    private final FieldType thriftType;
    private final com.zoomdata.edc.server.core.ThriftTypeFunction rsFunction;

    public Meta(FieldType thriftType, com.zoomdata.edc.server.core.ThriftTypeFunction rsFunction) {
        this.thriftType = thriftType;
        this.rsFunction = rsFunction;
    }

    public static com.zoomdata.edc.server.core.Meta from(FieldType thriftType, com.zoomdata.edc.server.core.ThriftTypeFunction rsFunction) {
        return new com.zoomdata.edc.server.core.Meta(thriftType, rsFunction);
    }

    public FieldType getThriftType() {
        return thriftType;
    }

    public ThriftTypeFunction getRsFunction() {
        return rsFunction;
    }
}
