/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

public final class CollectionUtils {
    private CollectionUtils() { }

    public static boolean isEmpty(Collection c) {
        return c == null || c.isEmpty();
    }

    public static boolean isNotEmpty(Collection c) {
        return !isEmpty(c);
    }

    public static <T> List<T> union(List<T>... lists) {
        return stream(lists).flatMap(l -> l.stream()).collect(Collectors.toList());
    }
}
