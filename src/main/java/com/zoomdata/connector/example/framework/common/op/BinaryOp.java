/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.op;

public interface BinaryOp<A, B, C> {
    C apply(A a, B b);
}
