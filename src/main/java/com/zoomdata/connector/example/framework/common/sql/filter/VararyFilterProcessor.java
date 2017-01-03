/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class VararyFilterProcessor extends AbstractFilterProcessor {

    public VararyFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                 FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
    }

    @Override
    protected BooleanExpression processIntegerFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        NumberPath numberPath = getFilterTypeService().getIntegerTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        List<Object> values = extractValues(filter).stream()
            .map(field -> getFilterTypeService().getIntegerTypeResolver().extract(field, fieldMetadata))
            .collect(Collectors.toList());
        return processInteger(numberPath, values);
    }

    @Override
    protected BooleanExpression processDoubleFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        NumberPath numberPath = getFilterTypeService().getDoubleTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        List<Object> values = extractValues(filter).stream().map(field ->
            getFilterTypeService().getDoubleTypeResolver().extract(field, fieldMetadata))
            .collect(Collectors.toList());
        return processDouble(numberPath, values);
    }

    @Override
    protected BooleanExpression processStringFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        StringPath stringPath = getFilterTypeService().getStringTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        List<Object> values = extractValues(filter).stream().map(field ->
            getFilterTypeService().getStringTypeResolver().extract(field, fieldMetadata))
            .collect(Collectors.toList());
        return processString(stringPath, values);
    }

    @Override
    protected BooleanExpression processDateFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        DateTimePath dateTimePath = getFilterTypeService().getDateTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        List<Object> values = extractValues(filter).stream().map(field ->
            getFilterTypeService().getDateTypeResolver().extract(field, fieldMetadata))
            .collect(Collectors.toList());
        return processDate(dateTimePath, values);
    }

    protected abstract String extractPath(Filter filter);
    protected abstract List<Field> extractValues(Filter filter);

    protected abstract BooleanExpression processInteger(NumberPath numberPath, List<Object> values);
    protected abstract BooleanExpression processDouble(NumberPath numberPath, List<Object> values);
    protected abstract BooleanExpression processString(StringPath stringPath, List<Object> values);
    protected abstract BooleanExpression processDate(DateTimePath dateTimePath, List<Object> values);
}
