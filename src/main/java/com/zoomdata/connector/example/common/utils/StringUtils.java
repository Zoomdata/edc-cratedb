/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

public final class StringUtils {

    public static final String EMPTY = "";

    private StringUtils() { }

    public static boolean isEmpty(String s) {
        return s == null || EMPTY.equals(s);
    }

    public static String extractType(String s) {
        int typeIndex = s.indexOf('(');
        String processed = s;
        if (typeIndex != -1) { // found
            processed = s.substring(0, typeIndex);
        }
        return processed;
    }

}
