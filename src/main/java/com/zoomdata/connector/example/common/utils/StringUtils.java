/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import com.google.common.escape.Escapers;

import java.util.Set;

public final class StringUtils {

    public static final String EMPTY = "";
    private static final String SPACE = " ";

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

    /**
     * returns string before last entry of separator
     * in case string doesn't contain separator -- returns whole string
     *
     * @param str       string that should be analyzed
     * @param separator string that should separate string.
     *                  Examples:
     *                  subStringBeforeLast("aaaQQaQQde","QQ") = "aaaaQQ"
     *                  subStringBeforeLast("aaaQQaQQde","test") = "aaaQQaQQde"
     */

    public static String subStringBeforeLast(String str, String separator) {
        int from = str.lastIndexOf(separator);
        return from == -1 ? str : str.substring(0, from);
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

    public static String prependEachCharacterWithString(String input, Set<Character> charsToPrepend, String prefix) {
        Escapers.Builder escaperBuilder = Escapers.builder();
        charsToPrepend.forEach(
                character -> escaperBuilder.addEscape(character, prefix + character)
        );
        return escaperBuilder.build().escape(input);
    }
}