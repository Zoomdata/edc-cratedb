/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.op;

public interface UnaryOp<A, B> {
    B apply(A a);
}
