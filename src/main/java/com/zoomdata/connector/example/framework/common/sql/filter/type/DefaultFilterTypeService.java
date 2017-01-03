/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter.type;

public class DefaultFilterTypeService implements FilterTypeService {

    private final DoubleFilterTypeResolver doubleFilterTypeResolver = new DoubleFilterTypeResolver();
    private final StringFilterTypeResolver stringFilterTypeResolver = new StringFilterTypeResolver();
    private final DateFilterTypeResolver dateFilterTypeResolver = new DateFilterTypeResolver();
    private IntegerFilterTypeResolver integerFilterTypeResolver = new IntegerFilterTypeResolver();

    @Override
    public FilterTypeResolver getIntegerTypeResolver() {
        return integerFilterTypeResolver;
    }

    @Override
    public FilterTypeResolver getDoubleTypeResolver() {
        return doubleFilterTypeResolver;
    }

    @Override
    public FilterTypeResolver getStringTypeResolver() {
        return stringFilterTypeResolver;
    }

    @Override
    public FilterTypeResolver getDateTypeResolver() {
        return dateFilterTypeResolver;
    }
}
