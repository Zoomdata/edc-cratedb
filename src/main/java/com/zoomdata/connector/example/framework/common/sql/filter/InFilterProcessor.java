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
import com.zoomdata.gen.edc.types.FieldType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InFilterProcessor extends VararyFilterProcessor {

    public InFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                             FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
    }

    @Override
    protected FieldType extractFieldType(Filter filter) {
        return filter.getFilterIN().getType();
    }

    @Override
    protected String extractPath(Filter filter) {
        return filter.getFilterIN().getPath();
    }

    @Override
    protected List<Field> extractValues(Filter filter) {
        return filter.getFilterIN().getValues();
    }

    @Override
    protected BooleanExpression processInteger(NumberPath numberPath, List<Object> values) {
        return numberPath.in(values);
    }

    @Override
    protected BooleanExpression processDouble(NumberPath numberPath, List<Object> values) {
        return numberPath.in(values);
    }

    @Override
    protected BooleanExpression processString(StringPath stringPath, List<Object> values) {
        return stringPath.in(values.stream().map(Object::toString).collect(Collectors.toList()));
    }

    @Override
    protected BooleanExpression processDate(DateTimePath dateTimePath, List<Object> values) {
        return dateTimePath.in(values);
    }
}
