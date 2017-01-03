/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.zoomdata.gen.edc.sort.RawSort;

import java.util.List;


public interface RawSortsProcessor {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param sorts list of {@link RawSort}.
     * @return list of {@link OrderSpecifier} for ORDER BY.
     */
    List<OrderSpecifier> process(Path<?> table, List<RawSort> sorts);

    /**
     * Returns table.
     * @return table or <code>null</code> if processor hasn't been initialized.
     */
    Path<?> getTable();

    /**
     * Returns list of processed sorts.
     * @return list of sorts or <code>null</code> if processor hasn't been initialized.
     */
    List<RawSort> getThriftSorts();

    /**
     * Returns result of {@link #process(Path, List)} method execution.
     * @return list of {@link OrderSpecifier} or <code>null</code> if processor hasn't been initialized.
     */
    List<OrderSpecifier> getOrderBy();
}
