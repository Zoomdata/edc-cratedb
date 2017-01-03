/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.sql.SQLQuery;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.metric.Metric;

import java.util.List;

public interface MetricsProcessor {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param metrics metrics.
     * @return list of expressions for SELECT.
     */
    List<ComparableExpressionBase> process(Path<?> table, List<Metric> metrics);

    /**
     * Complex expressions may require self-joins and subqueries. In this method processor may implement logic
     * or joining to the main table.
     * @param sqlQueryBuilder SQLQueryBuilder. May be used to create processors required by subqueries using its
     *                        factory methods.
     * @param sqlQuery main query.
     * @param thriftFilters may be used to as filters in case when filtering in subquery
     *                      must be the same as in main query.
     * @param groupsProcessor groups processor used in main query. May be used to get groups in case when grouping
     *                         in subquery must be the same as in main query.
     */
    void doJoins(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                 List<Filter> thriftFilters, IGroupExpressionProducer groupsProcessor);

    /**
     * Returns table.
     * @return table or <code>null</code> if processor hasn't been initialized.
     */
    Path<?> getTable();

    /**
     * Returns list of processed metrics.
     * @return list of metrics or <code>null</code> if processor hasn't been initialized.
     */
    List<Metric> getThriftMetrics();

    /**
     * Returns result of {@link #process(Path, List)} method execution.
     * @return list of SELECT expressions or <code>null</code> if processor hasn't been initialized.
     */
    List<ComparableExpressionBase> getSelect();

    /**
     * Returns SELECT expression by processed metric.
     * @return expression or <code>null</code> if processor hasn't been initialized or
     * there is no expression for this metric.
     */
    ComparableExpressionBase getMetricExpression(Metric metric);

    /**
     * Returns alias to use for row number in percentile subquery
     * @param percentileField
     * @return
     */
    String createPercentileRowNumberAliasName(String percentileField);
}
