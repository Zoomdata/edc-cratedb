/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils.struct;

public final class Pair<T1, T2> {
    private final T1 left;
    private final T2 right;

    private Pair(T1 left, T2 right) {
        this.left = left;
        this.right = right;
    }

    public static <A, B> Pair<A, B> create(A left, B right) {
        return new Pair<>(left, right);
    }

    public T1 getLeft() {
        return left;
    }

    public T2 getRight() {
        return right;
    }
}
