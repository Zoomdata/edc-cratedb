/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter.type;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.Expressions;
import com.zoomdata.connector.example.common.utils.ThriftUtils;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;
import org.joda.time.DateTime;

public class DateFilterTypeResolver implements FilterTypeResolver<DateTime, DateTimePath<DateTime>> {

    @Override
    public DateTime extract(Field field, FieldMetadata fieldMetadata) {
        return ThriftUtils.getDateTime(field);
    }

    @Override
    public DateTimePath<DateTime> buildPath(Path<?> table, String path, FieldMetadata fieldMetadata) {
        return Expressions.dateTimePath(DateTime.class, table, path);
    }

}
