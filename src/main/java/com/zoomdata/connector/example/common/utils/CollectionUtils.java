/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils;

import com.zoomdata.connector.example.common.utils.struct.Pair;
import com.zoomdata.gen.edc.types.ListField;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class CollectionUtils {
    private CollectionUtils() {
    }

    public static <T1, T2> Map<T1, T2> merge(Map<T1, T2> mapPriorityLeast, Map<T1, T2> mapPriorityFirst) {
        Map<T1, T2> newMap = new HashMap<>();

        for (Map.Entry<T1, T2> entry : mapPriorityLeast.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }

        // override
        for (Map.Entry<T1, T2> entry : mapPriorityFirst.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }
        return newMap;
    }

    public static int sumInts(int... numbers) {
        int result = 0;
        for (Integer i : numbers) {
            result += i;
        }
        return result;
    }

    public static boolean isEmpty(Collection c) {
        return c == null || c.isEmpty();
    }

    public static boolean isNotEmpty(Collection c) {
        return !isEmpty(c);
    }

    public static List<ListField> collectionToListValues(Collection<? extends Object> collection) {
        return collection.stream()
                .map(value -> value == null ? new ListField().setIsNull(true) : new ListField().setValue(value.toString()))
                .collect(toList());
    }

    public static <T> List<T> union(List<T>... lists) {
        return stream(lists).flatMap(l -> l.stream()).collect(Collectors.toList());
    }

    public static <T, K, U, M extends Map<K, U>> Collector<T, ?, M> toSpecificMap(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends U> valueMapper,
            Supplier<M> mapSupplier) {
        return Collectors.toMap(
                keyMapper,
                valueMapper,
                (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                },
                mapSupplier);
    }

    public static <T> Stream<T> streamOfNullable(Collection<T> collection) {
        return Optional.ofNullable(collection).orElse(emptyList()).stream();
    }

    public static  <T> Stream<Pair<Integer, T>> enumerate(Stream<T> stream) {
        AtomicInteger incrementer = new AtomicInteger(0);
        return stream.map(e -> Pair.create(incrementer.incrementAndGet(), e));
    }
}
