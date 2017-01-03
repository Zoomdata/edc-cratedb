/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter.type;

import com.querydsl.core.types.Path;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;

public interface FilterTypeResolver<T, P extends Path<T>> {

    T extract(Field field, FieldMetadata fieldMetadata);

    P buildPath(Path<?> table, String path, FieldMetadata fieldMetadata);
}
