/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class AbstractFilterProcessor implements FilterProcessor {

    private Map<FieldType, Function<Filter, Predicate>> typedOperators = new HashMap<>();
    {
        typedOperators.put(FieldType.INTEGER, this::processIntegerFilter);
        typedOperators.put(FieldType.DOUBLE, this::processDoubleFilter);
        typedOperators.put(FieldType.STRING, this::processStringFilter);
        typedOperators.put(FieldType.DATE, this::processDateFilter);
    }

    private Path<?> table;
    private Map<String, FieldMetadata> metadata;
    private FilterTypeService filterTypeService;

    public AbstractFilterProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                   FilterTypeService filterTypeService) {
        this.table = table;
        this.metadata = metadata;
        this.filterTypeService = filterTypeService;
    }

    @Override
    public Predicate processFilter(Filter filter) {
        return typedOperators.get(extractFieldType(filter)).apply(filter);
    }

    protected abstract Predicate processIntegerFilter(Filter filter);

    protected abstract Predicate processDoubleFilter(Filter filter);

    protected abstract Predicate processStringFilter(Filter filter);

    protected abstract Predicate processDateFilter(Filter filter);

    protected abstract FieldType extractFieldType(Filter filter);

    protected FieldMetadata getFieldMetadata(String fieldName){
        return metadata.get(fieldName);
    }

    protected Path<?> getTable() {
        return table;
    }

    protected FilterTypeService getFilterTypeService() {
        return filterTypeService;
    }
}
