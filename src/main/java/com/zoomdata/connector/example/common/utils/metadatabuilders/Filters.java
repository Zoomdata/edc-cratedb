/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils.metadatabuilders;

import com.zoomdata.gen.edc.filter.*;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.List;
import java.util.stream.Collectors;

import static com.zoomdata.gen.edc.filter.FilterFunction.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;

public class Filters {
    public static Filter leDouble(String field, double value) {
        return le(field, String.valueOf(value), FieldType.DOUBLE);
    }

    public static Filter leInt(String field, long value) {
        return le(field, String.valueOf(value), FieldType.INTEGER);
    }

    public static Filter leDate(String field, String value) {
        return le(field, value, FieldType.DATE);
    }

    public static Filter ltDouble(String field, double value) {
        return lt(field, String.valueOf(value), FieldType.DOUBLE);
    }

    public static Filter ltInt(String field, long value) {
        return lt(field, String.valueOf(value), FieldType.INTEGER);
    }

    public static Filter ltDate(String field, String value) {
        return lt(field, String.valueOf(value), FieldType.DATE);
    }

    public static Filter geDouble(String field, double value) {
        return ge(field, String.valueOf(value), FieldType.DOUBLE);
    }

    public static Filter geInt(String field, long value) {
        return ge(field, String.valueOf(value), FieldType.INTEGER);
    }

    public static Filter geDate(String field, String value) {
        return ge(field, value, FieldType.DATE);
    }

    public static Filter gtDouble(String field, double value) {
        return gt(field, String.valueOf(value), FieldType.DOUBLE);
    }

    public static Filter gtInt(String field, long value) {
        return gt(field, String.valueOf(value), FieldType.INTEGER);
    }

    public static Filter gtDate(String field, String value) {
        return gt(field, value, FieldType.DATE);
    }

    public static Filter eqStr(String field, String value) {
        return eq(field, value, FieldType.STRING);
    }

    public static Filter eqDouble(String field, double value) {
        return eq(field, String.valueOf(value), FieldType.DOUBLE);
    }

    public static Filter eqInt(String field, long value) {
        return eq(field, String.valueOf(value), FieldType.INTEGER);
    }

    public static Filter eqDate(String field, String value) {
        return eq(field, value, FieldType.DATE);
    }

    public static Filter inStr(String field, String... values) {
        return in(field, FieldType.STRING, values);
    }

    public static Filter inDouble(String field, Double... values) {
        return in(field, FieldType.DOUBLE, stream(values).map(v -> String.valueOf(v)).toArray(String[]::new));
    }

    public static Filter inDate(String field, String... values) {
        return in(field, FieldType.DATE, values);
    }

    public static Filter inInt(String field, Integer... values) {
        return in(field, FieldType.INTEGER, stream(values).map(v -> String.valueOf(v)).toArray(String[]::new));
    }

    public static Filter inInt(String field, Long... values) {
        return in(field, FieldType.INTEGER, stream(values).map(v -> String.valueOf(v)).toArray(String[]::new));
    }

    public static Filter isNullStr(String field) {
        return isNull(field, FieldType.STRING);
    }

    public static Filter isNullInt(String field) {
        return isNull(field, FieldType.INTEGER);
    }

    public static Filter isNullDouble(String field) {
        return isNull(field, FieldType.DOUBLE);
    }

    public static Filter isNullDate(String field) {
        return isNull(field, FieldType.DATE);
    }

    public static Filter ge(String field, String value, FieldType type) {
        return new Filter(FilterFunction.GE)
                .setFilterGE(new FilterGE()
                        .setPath(field)
                        .setType(type)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter gt(String field, String value, FieldType type) {
        return new Filter(FilterFunction.GT)
                .setFilterGT(new FilterGT()
                        .setPath(field)
                        .setType(type)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter lt(String field, String value, FieldType type) {
        return new Filter(FilterFunction.LT)
                .setFilterLT(new FilterLT()
                        .setPath(field)
                        .setType(type)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter le(String field, String value, FieldType type) {
        return new Filter(FilterFunction.LE)
                .setFilterLE(new FilterLE()
                        .setPath(field)
                        .setType(type)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter eq(String field, String value, FieldType type) {
        return new Filter(FilterFunction.EQ)
                .setFilterEQ(new FilterEQ()
                        .setPath(field)
                        .setType(type)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter isNull(String field, FieldType type) {
        return new Filter(FilterFunction.IS_NULL)
                .setFilterISNULL(new FilterISNULL()
                        .setType(type)
                        .setPath(field)
                );
    }

    public static Filter eqi(String field, String value) {
        return new Filter(FilterFunction.EQI)
                .setFilterEQI(new FilterEQI()
                        .setPath(field)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter in(String field, FieldType type, String... values) {
        return new Filter(FilterFunction.IN)
                .setFilterIN(new FilterIN()
                        .setPath(field)
                        .setType(type)
                        .setValues(stream(values)
                                .map(v -> new Field().setValue(v))
                                .collect(Collectors.toList())));
    }

    public static Filter contains(String field, String value) {
        return new Filter(FilterFunction.CONTAINS)
                .setFilterCONTAINS(new FilterCONTAINS()
                        .setPath(field)
                        .setValue(new Field().setValue(value))
                );
    }

    public static Filter textSearch(String field, String value) {
        return new Filter(FilterFunction.TEXT_SEARCH)
                .setFilterTEXT_SEARCH(new FilterTEXT_SEARCH()
                        .setPath(field)
                        .setValue(value)
                );
    }

    public static Filter textSearch(String value) {
        return new Filter(FilterFunction.TEXT_SEARCH)
                .setFilterTEXT_SEARCH(new FilterTEXT_SEARCH()
                        .setValue(value)
                );
    }

    public static Filter not(Filter filter) {
        return new Filter(FilterFunction.NOT)
                .setFilterNOT(new FilterNOT()
                        .setFilter(filter));
    }

    public static Filter or(Filter... subFilters) {
        return or(asList(subFilters));
    }

    public static Filter or(List<Filter> subFilters) {
        return new Filter(FilterFunction.OR)
                .setFilterOR(new FilterOR()
                        .setFilters(subFilters));
    }

    public static Filter and(Filter... subFilters) {
        return and(asList(subFilters));
    }

    public static Filter and(List<Filter> subFilters) {
        return new Filter(FilterFunction.AND)
                .setFilterAND(new FilterAND()
                        .setFilters(subFilters));
    }

    public static Filter newCompositeFilter(FilterFunction type, List<Filter> filters) {
        switch (type) {
            case OR:
                return new Filter(OR).setFilterOR(new FilterOR(filters));
            case AND:
                return new Filter(AND).setFilterAND(new FilterAND(filters));
            case NOT:
                return new Filter(NOT).setFilterNOT(new FilterNOT(filters.get(0)));
            default:
                throw new IllegalArgumentException(format("Filter of type [%s] can't contain inner filters: %s", type, filters));
        }
    }

    public static List<Filter> getInnerFilters(Filter filter) {
        switch (filter.getType()) {
            case OR:
                return filter.getFilterOR().getFilters();
            case AND:
                return filter.getFilterAND().getFilters();
            case NOT:
                return asList(filter.getFilterNOT().getFilter());
            default:
                return emptyList();
        }
    }
}