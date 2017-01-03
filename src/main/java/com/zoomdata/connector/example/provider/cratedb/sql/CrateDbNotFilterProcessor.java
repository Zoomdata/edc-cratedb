/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb.sql;

import com.querydsl.core.types.Predicate;
import com.zoomdata.connector.example.framework.common.sql.filter.FilterProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.NotFilterProcessor;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterFunction;
import com.zoomdata.gen.edc.filter.FilterISNULL;
import com.zoomdata.gen.edc.filter.FilterOR;
import com.zoomdata.gen.edc.types.FieldType;

public class CrateDbNotFilterProcessor extends NotFilterProcessor {

    private FilterProcessor filtersProcessorRouter;

    public CrateDbNotFilterProcessor(FilterProcessor filtersProcessorRouter) {
        super(filtersProcessorRouter);
        this.filtersProcessorRouter = filtersProcessorRouter;
    }

    /*  When negating most filters, CrateDB will include NULL values such that, we have 1,500 records
     *  The column string_null has 15 values = 'Alaska', 453 nulls, and the rest other values. When we
     *
     *  select count(*) FROM myTable WHERE not string_field = 'Alaska'
     *
     *  CrateDB gives us 1,485, while Zoomdata expects 1,032, i.e., Zoomdata expects not to count the nulls. So
     *  when we have a NOT filter we have to adjust the filter to look like
     *
     *  select count(*) FROM myTable WHERE not (string_field = 'Alaska' OR string_field is null)
      * */
    @Override
    public Predicate processFilter(Filter filter) {
        Filter filterForNot = filter.getFilterNOT().getFilter();
        Filter adjustedFilter = filterForNot;
        // Only apply to operators it could affect
        if(filterHasAffectedClause(filterForNot)) {
            // Type doesn't matter for a NULL filter, so use anything the filter processor will accept
            Filter filterIsNull = new Filter().setType(FilterFunction.IS_NULL)
                    .setFilterISNULL(new FilterISNULL().setPath(extractFilterPath(filterForNot)).setType(FieldType.STRING));

            // Create an OR filter that takes the original condition and will also use IS NULL
            FilterOR filterOr = new FilterOR();
            filterOr.addToFilters(filterForNot);
            filterOr.addToFilters(filterIsNull);
            adjustedFilter = new Filter().setType(FilterFunction.OR).setFilterOR(filterOr);
        }
        return filtersProcessorRouter.processFilter(adjustedFilter).not();
    }

    private static boolean filterHasAffectedClause(Filter filter) {
        return filter.isSetFilterEQ() || filter.isSetFilterEQI() || filter.isSetFilterGE() || filter.isSetFilterCONTAINS() ||
                filter.isSetFilterENDS_WITH() || filter.isSetFilterGT() || filter.isSetFilterIN() || filter.isSetFilterLE() ||
                filter.isSetFilterLT() || filter.isSetFilterSTARTS_WITH() || filter.isSetFilterTEXT_SEARCH();
    }

    private static String extractFilterPath(Filter filter) {
        if(filter.isSetFilterEQ()) {
            return filter.getFilterEQ().getPath();
        } else if (filter.isSetFilterEQI()) {
            return filter.getFilterEQI().getPath();
        } else if (filter.isSetFilterCONTAINS()) {
            return filter.getFilterCONTAINS().getPath();
        } else if (filter.isSetFilterGE()) {
            return filter.getFilterGE().getPath();
        } else if (filter.isSetFilterGT()) {
            return filter.getFilterGT().getPath();
        } else if (filter.isSetFilterLE()) {
            return filter.getFilterLE().getPath();
        } else if (filter.isSetFilterLT()) {
            return filter.getFilterLT().getPath();
        } else if (filter.isSetFilterIN()) {
            return filter.getFilterIN().getPath();
        } else if (filter.isSetFilterSTARTS_WITH()) {
            return filter.getFilterSTARTS_WITH().getPath();
        } else if (filter.isSetFilterENDS_WITH()) {
            return filter.getFilterENDS_WITH().getPath();
        } else if (filter.isSetFilterTEXT_SEARCH()) {
            return filter.getFilterTEXT_SEARCH().getPath();
        }
        throw new IllegalArgumentException("Invalid filter type for altering NOT clause");
    }
}
