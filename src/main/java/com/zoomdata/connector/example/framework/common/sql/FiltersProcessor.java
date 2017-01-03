/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Predicate;
import com.zoomdata.gen.edc.filter.Filter;

import java.util.List;

public interface FiltersProcessor {
    /**
     * Initializes processor and does all work.
     * @param filters filters.
     * @return predicate for WHERE.
     */
    Predicate process(List<Filter> filters);

    /**
     * Returns result of {@link #process(List)} method execution.
     * @return WHERE predicate or <code>null</code> if processor hasn't been initialized.
     */
    Predicate getWhere();
}
