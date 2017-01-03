/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.zoomdata.gen.edc.request.StatField;

import java.util.List;


public interface StatsProcessor {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param stats stats.
     * @return list of expressions for SELECT.
     */
    List<ComparableExpressionBase> process(Path<?> table, List<StatField> stats);

    /**
     * Returns table.
     * @return table or <code>null</code> if processor hasn't been initialized.
     */
    Path<?> getTable();

    /**
     * Returns list of processed stats.
     * @return list of stats or <code>null</code> if processor hasn't been initialized.
     */
    List<StatField> getThriftStats();

    /**
     * Returns result of {@link #process(Path, List)} method execution.
     * @return list of SELECT expressions or <code>null</code> if processor hasn't been initialized.
     */
    List<ComparableExpressionBase> getSelect();
}
