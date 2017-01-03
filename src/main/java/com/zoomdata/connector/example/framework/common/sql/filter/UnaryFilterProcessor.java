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
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.Map;

public abstract class UnaryFilterProcessor extends AbstractFilterProcessor {

    public UnaryFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
    }

    @Override
    protected BooleanExpression processIntegerFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        NumberPath numberPath = getFilterTypeService().getIntegerTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        return processInteger(numberPath);
    }

    @Override
    protected BooleanExpression processDoubleFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        NumberPath numberPath = getFilterTypeService().getDoubleTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        return processDouble(numberPath);
    }

    @Override
    protected BooleanExpression processStringFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        StringPath stringPath = getFilterTypeService().getStringTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        return processString(stringPath);
    }

    @Override
    protected BooleanExpression processDateFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        DateTimePath dateTimePath = getFilterTypeService().getDateTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        return processDate(dateTimePath);
    }

    protected abstract FieldType extractFieldType(Filter filter);
    protected abstract String extractPath(Filter filter);

    protected abstract BooleanExpression processInteger(NumberPath numberPath);
    protected abstract BooleanExpression processDouble(NumberPath numberPath);
    protected abstract BooleanExpression processString(StringPath stringPath);
    protected abstract BooleanExpression processDate(DateTimePath dateTimePath);
}
