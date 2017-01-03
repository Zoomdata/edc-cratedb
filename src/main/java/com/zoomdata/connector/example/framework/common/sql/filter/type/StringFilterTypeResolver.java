/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter.type;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.zoomdata.connector.example.common.utils.ThriftUtils;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;

public class StringFilterTypeResolver implements FilterTypeResolver<String, StringPath> {

    @Override
    public String extract(Field field, FieldMetadata fieldMetadata) {
        return ThriftUtils.getString(field);
    }

    @Override
    public StringPath buildPath(Path<?> table, String path, FieldMetadata fieldMetadata) {
        return Expressions.stringPath(table, path);
    }

}
