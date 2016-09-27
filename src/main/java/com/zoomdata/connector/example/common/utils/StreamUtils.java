/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import java.util.function.Predicate;

public class StreamUtils {
    public static <T> Predicate<T> not(Predicate<T> t) {
        return t.negate();
    }

}
