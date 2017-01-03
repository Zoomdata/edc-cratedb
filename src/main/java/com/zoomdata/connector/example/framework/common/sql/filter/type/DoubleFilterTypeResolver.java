/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter.type;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberPath;
import com.zoomdata.connector.example.common.utils.ThriftUtils;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;

public class DoubleFilterTypeResolver implements FilterTypeResolver<Double, NumberPath<Double>> {

    @Override
    public Double extract(Field field, FieldMetadata fieldMetadata) {
        return ThriftUtils.getDouble(field);
    }

    @Override
    public NumberPath<Double> buildPath(Path<?> table, String path, FieldMetadata fieldMetadata) {
        return Expressions.numberPath(Double.class, table, path);
    }

}
