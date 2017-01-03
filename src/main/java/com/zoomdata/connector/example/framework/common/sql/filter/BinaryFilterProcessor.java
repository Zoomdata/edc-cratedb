/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.Map;

import static com.querydsl.core.types.dsl.Expressions.constant;

public abstract class BinaryFilterProcessor extends AbstractFilterProcessor {

    public BinaryFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                 FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
    }

    @Override
    protected Predicate processIntegerFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        NumberPath numberPath = getFilterTypeService().getIntegerTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        Expression constant = constant(getFilterTypeService().getIntegerTypeResolver()
            .extract(extractValue(filter), fieldMetadata));
        return processInteger(numberPath, constant);
    }

    @Override
    protected Predicate processDoubleFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        NumberPath numberPath = getFilterTypeService().getDoubleTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        Expression constant = constant(getFilterTypeService().getDoubleTypeResolver()
            .extract(extractValue(filter), fieldMetadata));
        return processDouble(numberPath, constant);
    }

    @Override
    protected Predicate processStringFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        StringPath stringPath = getFilterTypeService().getStringTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
            Expression constant = constant(getFilterTypeService().getStringTypeResolver()
                .extract(extractValue(filter), fieldMetadata));
        return processString(stringPath, constant);
    }

    @Override
    protected Predicate processDateFilter(Filter filter) {
        String path = extractPath(filter);
        FieldMetadata fieldMetadata = getFieldMetadata(path);
        DateTimePath dateTimePath = getFilterTypeService().getDateTypeResolver()
            .buildPath(getTable(), path, fieldMetadata);
        Expression constant = constant(getFilterTypeService().getDateTypeResolver()
            .extract(extractValue(filter), fieldMetadata));
        return processDate(dateTimePath, constant);
    }

    protected abstract String extractPath(Filter filter);
    protected abstract Field extractValue(Filter filter);

    protected abstract BooleanExpression processInteger(NumberPath numberPath, Expression expression);
    protected abstract BooleanExpression processDouble(NumberPath numberPath, Expression expression);
    protected abstract BooleanExpression processString(StringPath stringPath, Expression expression);
    protected abstract BooleanExpression processDate(DateTimePath dateTimePath, Expression expression);
}
