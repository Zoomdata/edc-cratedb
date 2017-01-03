/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.zoomdata.connector.example.framework.common.sql.FiltersProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.AndFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.ContainsFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.EqFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.EqiFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.FilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.GeFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.GtFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.InFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.IsNullFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.LeFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.LtFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.NotFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.OrFilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterFunction;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DefaultFiltersProcessor implements FiltersProcessor, FilterProcessor {

    private Path<?> table;
    private Map<String, FieldMetadata> metadata;
    private BooleanBuilder where = new BooleanBuilder();

    private Map<FilterFunction, FilterProcessor> filterProcessors = new HashMap<>();

    public DefaultFiltersProcessor(Path<?> table, Map<String, FieldMetadata> metadata,
                                   FilterTypeService filterTypeService) {
        this.table = table;
        this.metadata = metadata;
        addFilterProcessor(FilterFunction.AND,  new AndFilterProcessor(this));
        addFilterProcessor(FilterFunction.OR, new OrFilterProcessor(this));
        addFilterProcessor(FilterFunction.NOT, new NotFilterProcessor(this));
        addFilterProcessor(FilterFunction.EQ, new EqFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.LT, new LtFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.LE, new LeFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.GT, new GtFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.GE, new GeFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.EQI, new EqiFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.CONTAINS, new ContainsFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.IS_NULL, new IsNullFilterProcessor(table, metadata, filterTypeService));
        addFilterProcessor(FilterFunction.IN, new InFilterProcessor(table, metadata, filterTypeService));
    }

    protected void addFilterProcessor(FilterFunction filterFunction,
                                      FilterProcessor filterProcessor) {
        filterProcessors.put(filterFunction, filterProcessor);
    }

    @Override
    public Predicate process(List<Filter> filters) {
        for (Filter filter : filters) {
            where.and(processFilter(filter));
        }
        return where;
    }

    @Override
    public Predicate getWhere() {
        return where;
    }

    @Override
    public Predicate processFilter(Filter filter) {
        return filterProcessors.get(filter.getType()).processFilter(filter);
    }
}
