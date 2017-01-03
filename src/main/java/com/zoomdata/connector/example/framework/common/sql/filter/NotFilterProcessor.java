/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.Predicate;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterNOT;

public class NotFilterProcessor implements FilterProcessor {

    private FilterProcessor filtersProcessorRouter;

    public NotFilterProcessor(FilterProcessor filtersProcessorRouter) {
        this.filtersProcessorRouter = filtersProcessorRouter;
    }

    @Override
    public Predicate processFilter(Filter filter) {
        FilterNOT filterNOT = filter.getFilterNOT();
        Filter filterForNot = filterNOT.getFilter();;
        return filtersProcessorRouter.processFilter(filterForNot).not();
    }
}
