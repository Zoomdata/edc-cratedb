/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.filter;

import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterOR;

public class OrFilterProcessor implements FilterProcessor {

    private FilterProcessor filtersProcessorRouter;

    public OrFilterProcessor(FilterProcessor filtersProcessorRouter) {
        this.filtersProcessorRouter = filtersProcessorRouter;
    }

    @Override
    public Predicate processFilter(Filter filter) {
        FilterOR filterOR = filter.getFilterOR();
        Predicate[] subfilters = filterOR.getFilters().stream()
            .map(subfilter -> filtersProcessorRouter.processFilter(subfilter))
            .toArray(Predicate[]::new);

        return ExpressionUtils.anyOf(subfilters);
    }
}
