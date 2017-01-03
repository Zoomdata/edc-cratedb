/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.Expression;
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

import java.util.Map;

public class EqiFilterProcessor extends BinaryFilterProcessor {

    public EqiFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                              FilterTypeService filterTypeService) {
        super(table, metadata, filterTypeService);
    }

    @Override
    protected FieldType extractFieldType(Filter filter) {
        return FieldType.STRING;
    }

    @Override
    protected String extractPath(Filter filter) {
        return filter.getFilterEQI().getPath();
    }

    @Override
    protected Field extractValue(Filter filter) {
        return filter.getFilterEQI().getValue();
    }

    @Override
    protected BooleanExpression processInteger(NumberPath numberPath, Expression expression) {
        throw new UnsupportedOperationException("Eqi filter does not support integer type");
    }

    @Override
    protected BooleanExpression processDouble(NumberPath numberPath, Expression expression) {
        throw new UnsupportedOperationException("Eqi filter does not support double type");
    }

    @Override
    protected BooleanExpression processString(StringPath stringPath, Expression expression) {
        return stringPath.equalsIgnoreCase(expression);
    }

    @Override
    protected BooleanExpression processDate(DateTimePath dateTimePath, Expression expression) {
        throw new UnsupportedOperationException("Eqi filter does not support date type");
    }
}
