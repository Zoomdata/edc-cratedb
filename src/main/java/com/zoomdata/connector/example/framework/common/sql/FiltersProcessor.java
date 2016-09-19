/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.zoomdata.gen.edc.filter.Filter;

import java.util.List;

public interface FiltersProcessor {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param filters filters.
     * @return predicate for WHERE.
     */
    Predicate process(Path<?> table, List<Filter> filters);

    /**
     * Returns table.
     * @return table or <code>null</code> if processor hasn't been initialized.
     */
    Path<?> getTable();

    /**
     * Returns list of processed filters.
     * @return list of filters or <code>null</code> if processor hasn't been initialized.
     */
    List<Filter> getThriftFilters();

    /**
     * Returns result of {@link #process(Path, List)} method execution.
     * @return WHERE predicate or <code>null</code> if processor hasn't been initialized.
     */
    Predicate getWhere();
}
