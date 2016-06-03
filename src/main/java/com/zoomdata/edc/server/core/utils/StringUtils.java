/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.utils;

public final class StringUtils {

    public static final String EMPTY = "";
    private static final String SPACE = " ";

    public static final String YEAR_FORMAT = "yyyy";
    public static final String SECONDS_FORMAT = "sssssssss";

    private StringUtils() { }

    public static boolean isEmpty(String s) {
        return s == null || EMPTY.equals(s);
    }

    public static String space(int n) {
        return repeat(n, SPACE);
    }

    public static String repeat(int n, String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    public static String prependWithSpaces(String s, int n) {
        StringBuilder sb = new StringBuilder();
        String[] lines = s.split("\\n");
        for (int i = 0; i < lines.length; i++) {
            sb.append(space(n) + lines[i]);
            if (i < lines.length - 1) {
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public static String extractType(String s) {
        int typeIndex = s.indexOf('(');
        String processed = s;
        if (typeIndex != -1) { // found
            processed = s.substring(0, typeIndex);
        }
        return processed;
    }

    public static int length(String schema) {
        return isEmpty(schema) ? 0 : schema.length();
    }

    public static String toLowerOrNull(String s) {
        return s == null ? null : s.toLowerCase();
    }

    public static String toUpperOrNull(String s) {
        return s == null ? null : s.toUpperCase();
    }

    public static boolean equals(String timeFieldPattern, String yearFormat) {
        if (timeFieldPattern == null && yearFormat == null) return true;
        if (timeFieldPattern == null || yearFormat == null) return false;
        return timeFieldPattern.equals(yearFormat);
    }
}
