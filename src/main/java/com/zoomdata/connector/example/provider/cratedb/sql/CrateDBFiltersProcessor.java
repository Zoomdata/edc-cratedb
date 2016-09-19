/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb.sql;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Path;
import com.zoomdata.connector.example.framework.common.sql.impl.DefaultFiltersProcessor;
import com.zoomdata.gen.edc.filter.*;
import com.zoomdata.gen.edc.types.FieldType;

public class CrateDBFiltersProcessor extends DefaultFiltersProcessor {

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
    protected void processNOT(Path<?> table, Filter filter, BooleanBuilder predicate) {
        Filter filterForNot = filter.getFilterNOT().getFilter();
        // Only apply to operators it could affect
        if(filterHasAffectedClause(filterForNot)) {
            // Type doesn't matter for a NULL filter, so use anything the filter processor will accept
            Filter filterIsNull = new Filter().setType(FilterFunction.IS_NULL)
                .setFilterISNULL(new FilterISNULL().setPath(extractFilterPath(filterForNot)).setType(FieldType.STRING));

            // Create an OR filter that takes the original condition and will also use IS NULL
            FilterOR filterOr = new FilterOR();
            filterOr.addToFilters(filterForNot);
            filterOr.addToFilters(filterIsNull);

            // Negate the filter such that NOT (field = someVal OR field is null)
            Filter outermostFilter = new Filter().setType(FilterFunction.NOT).setFilterNOT(
                new FilterNOT().setFilter(new Filter().setType(FilterFunction.OR).setFilterOR(filterOr)));

            // Process the new adjusted filter as normal
            super.processNOT(table, outermostFilter, predicate);
        } else {
            super.processNOT(table, filter, predicate);
        }
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
