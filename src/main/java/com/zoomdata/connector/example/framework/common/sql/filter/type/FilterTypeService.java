/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter.type;

import com.querydsl.core.types.dsl.DateTimePath;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.StringPath;

public interface FilterTypeService {

    <T extends Number & Comparable<?>> FilterTypeResolver<T, NumberPath<T>> getIntegerTypeResolver();
    <T extends Number & Comparable<?>> FilterTypeResolver<T, NumberPath<T>> getDoubleTypeResolver();
    FilterTypeResolver<String, StringPath> getStringTypeResolver();
    <T extends Comparable> FilterTypeResolver<T, DateTimePath<T>> getDateTypeResolver();

}
