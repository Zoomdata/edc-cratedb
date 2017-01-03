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

public class IsNullFilterProcessor extends UnaryFilterProcessor {

    public IsNullFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                 FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
    }

    @Override
    protected FieldType extractFieldType(Filter filter) {
        return filter.getFilterISNULL().getType();
    }

    @Override
    protected String extractPath(Filter filter) {
        return filter.getFilterISNULL().getPath();
    }

    @Override
    protected BooleanExpression processInteger(NumberPath numberPath) {
        return numberPath.isNull();
    }

    @Override
    protected BooleanExpression processDouble(NumberPath numberPath) {
        return numberPath.isNull();
    }

    @Override
    protected BooleanExpression processString(StringPath stringPath) {
        return stringPath.isNull();
    }

    @Override
    protected BooleanExpression processDate(DateTimePath dateTimePath) {
        return dateTimePath.isNull();
    }
}
