/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public final class MapUtils {
    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    private MapUtils() {}

    /**
     * returns map that doesn't contain listed keys
     *
     * @param map  map that should be filtered
     * @param keys keys that should be removed
     */
    public static <K, V> Map removeKeys(Map<K, V> map, String... keys) {
        Set<String> filterSet = Sets.newHashSet(keys);
        Map<K, V> result = new HashMap<>();
        map.entrySet()
                .stream()
                .filter(e -> !filterSet.contains(e.getKey()))
                .forEach(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }

    /**
     * returns map that contains only listed values
     *
     * @param map    map that should be filtered
     * @param values values that should be included in map
     */
    public static <K, V> Map filterValues(Map<K, V> map, V... values) {
        Map<K, V> result = new HashMap<>();
        Set<V> valueSet = Sets.newHashSet(values);
        map.entrySet()
                .stream()
                .filter(e -> valueSet.contains(e.getValue()))
                .forEach(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }

    /**
     * returns map with transformed keys
     * if Map is null -- it returns empty map,not null.
     *
     * @param map            map that should be transformed
     * @param keyTransformer function that transforms keys
     */
    public static <K, V> Map<K, V> transformKeys(Map<K, V> map, Function<K, K> keyTransformer) {

        if (MapUtils.isEmpty(map)) {
            return Collections.emptyMap();
        } else {
            Map<K, V> result = new HashMap<>();
            map.entrySet()
                    .stream()
                    .forEach(e -> result.put(keyTransformer.apply(e.getKey()), e.getValue()));
            return result;
        }
    }

    /**
     * Returns specified map if it is not <tt>null</tt>; otherwise returns empty map.
     *
     * <p>Calling this method ensures that returned map is never <tt>null</tt>
     * making all further operations performed on such map reference <tt>null</tt>-safe</p>
     *
     * @param map source map.
     * @param <K> key type.
     * @param <V> value type.
     * @return specified map if it is not <tt>null</tt>; otherwise returns empty map.
     */
    @Nonnull
    public static <K, V> Map<K, V> trimToEmpty(@Nullable Map<K, V> map) {
        if (null == map) {
            return Collections.emptyMap();
        } else {
            return map;
        }
    }

}
