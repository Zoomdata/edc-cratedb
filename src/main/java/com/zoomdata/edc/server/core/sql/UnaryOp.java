/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

public interface UnaryOp<A, B> {
    B apply(A a);
}
