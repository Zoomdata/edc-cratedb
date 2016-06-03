/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.Wildcard;
import com.querydsl.sql.RelationalPath;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;

import com.zoomdata.edc.server.core.utils.SQLUtils;
import com.zoomdata.edc.server.core.utils.StringUtils;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.metric.Metric;
import com.zoomdata.gen.edc.metric.MetricAvg;
import com.zoomdata.gen.edc.metric.MetricCount;
import com.zoomdata.gen.edc.metric.MetricDistinctCount;
import com.zoomdata.gen.edc.metric.MetricLastValue;
import com.zoomdata.gen.edc.metric.MetricMax;
import com.zoomdata.gen.edc.metric.MetricMin;
import com.zoomdata.gen.edc.metric.MetricPercentile;
import com.zoomdata.gen.edc.metric.MetricSum;
import com.zoomdata.gen.edc.metric.MetricType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.querydsl.core.types.dsl.Expressions.*;

public class MetricsProcessor {
    protected Path<?> table;
    protected List<Metric> thriftMetrics;
    protected List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> groupBy;

    List<ComparableExpressionBase> select;

    protected Map<Metric, ComparableExpressionBase> metricExpressions = new HashMap<>();

    protected Map<String, RelationalPath<?>> lastValueTables;
    protected Map<com.zoomdata.edc.server.core.sql.Percentile, RelationalPath<?>> percentileTables;

    public static final String LAST_VALUE_MAX_TIME_FIELD_ALIAS_PREFIX = "__max_";
    public static final String LAST_VALUE_SUBQUERY_ALIAS_PREFIX = "lv_";
    public static final String LAST_VALUE_FIELD_ALIAS_PREFIX = "__lv_";

    public static final String PERCENTILE_SUBQUERY_ALIAS_PREFIX = "prc_";
    public static final String PERCENTILE_FIELD_ALIAS_PREFIX = "__prc_";

    public List<ComparableExpressionBase> process(Path<?> table, List<Metric> metrics) {
        this.table = table;
        this.thriftMetrics = metrics;

        select = new ArrayList<>(metrics.size());

        for (Metric m : metrics) {
            MetricType type = m.getType();
            ComparableExpressionBase e;
            switch (type) {
                case COUNT: {
                    e = processCOUNT(m);
                    break;
                }
                case MIN: {
                    e = processMIN(m);
                    break;
                }
                case MAX: {
                    e = processMAX(m);
                    break;
                }
                case AVG: {
                    e = processAVG(m);
                    break;
                }
                case SUM: {
                    e = processSUM(m);
                    break;
                }
                case DISTINCT_COUNT: {
                    e = processDISTINCT_COUNT(m);
                    break;
                }
                case LAST_VALUE: {
                    e = processLAST_VALUE(m);
                    break;
                }
                case PERCENTILES: {
                    e = processPERCENTILES(m);
                    break;
                }
                default: {
                    throw new IllegalStateException("Metric of type " + type + " is not supported.");
                }

            }
            select.add(e);
            metricExpressions.put(m, e);
        }

        return select;
    }

    public void doJoins(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                        com.zoomdata.edc.server.core.sql.FiltersProcessor filtersProcessor, GroupsProcessor groupsProcessor) {
        if (lastValueTables != null) {
            joinLastValueTables(sqlQueryBuilder, sqlQuery, filtersProcessor, groupsProcessor);
        }
        if (percentileTables != null) {
            joinPercentileTables(sqlQueryBuilder, sqlQuery, filtersProcessor, groupsProcessor);
        }
    }

    public Path<?> getTable() {
        return table;
    }

    public List<Metric> getThriftMetrics() {
        return thriftMetrics;
    }

    public List<ComparableExpressionBase> getSelect() {
        return select;
    }

    public ComparableExpressionBase getMetricExpression(Metric metric) {
        return metricExpressions.get(metric);
    }

    // ==================== METRICS IMPLEMENTATION ====================

    protected RelationalPathBase<?> createTable(String alias) {
        if (table instanceof RelationalPathBase)
            return new RelationalPathBase<>(Object.class, alias, ((RelationalPathBase) table).getSchemaName(),
                ((RelationalPathBase) table).getTableName());
        else
            throw new UnsupportedOperationException("createTable needed (Percentiles, LastValues) not supported for CUSTOM_SQL");
    }

    protected ComparableExpressionBase processCOUNT(Metric m) {
        ComparableExpressionBase e;
        MetricCount metricCount = m.getCount();
        if (StringUtils.isEmpty(metricCount.getField())) {
            e = Wildcard.count;
        } else {
            e = numberPath(Long.class, table, metricCount.getField()).count();
        }
        return e;
    }

    protected ComparableExpressionBase processMIN(Metric m) {
        ComparableExpressionBase e;
        MetricMin metricMin = m.getMin();
        e = numberPath(Double.class, table, metricMin.getField()).min();
        return e;
    }

    protected ComparableExpressionBase processMAX(Metric m) {
        ComparableExpressionBase e;
        MetricMax metricMax = m.getMax();
        e = numberPath(Double.class, table, metricMax.getField()).max();
        return e;
    }

    protected ComparableExpressionBase processAVG(Metric m) {
        ComparableExpressionBase e;
        MetricAvg metricAvg = m.getAvg();
        e = numberPath(Double.class, table, metricAvg.getField()).avg();
        return e;
    }

    protected ComparableExpressionBase processSUM(Metric m) {
        ComparableExpressionBase e;
        MetricSum metricSum = m.getSum();
        e = numberPath(Double.class, table, metricSum.getField()).sum();
        return e;
    }

    protected ComparableExpressionBase processDISTINCT_COUNT(Metric m) {
        ComparableExpressionBase e;
        MetricDistinctCount metricDistinctCount = m.getDistinctCount();
        e = numberPath(Long.class, table, metricDistinctCount.getField()).countDistinct();
        return e;
    }

    protected ComparableExpressionBase processLAST_VALUE(Metric m) {
        MetricLastValue metricLastValue = m.getLastValue();
        String field = metricLastValue.getField();
        String timeField = metricLastValue.getTimeField();

        if (lastValueTables == null) {
            lastValueTables = new HashMap<>();
        }

        RelationalPath<?> lastValueTable = lastValueTables.get(timeField);
        if (lastValueTable == null) {
            lastValueTable = createTable(getLastValueSubqueryAliasPrefix() + (lastValueTables.size() + 1));
            lastValueTables.put(timeField, lastValueTable);
        }

        return cases()
                .when(dateTimePath(Date.class, table, timeField)
                        .eq(dateTimePath(Date.class, lastValueTable,
                                getLastValueMaxTimeFieldAliasPrefix() + timeField)))
                .then(stringPath(table, field))
                .otherwise(nullExpression())
                .max().as(getLastValueFieldAliasPrefix() + field + "_over_" +
                        SQLUtils.underscore(timeField));
    }

    protected ComparableExpressionBase processPERCENTILES(Metric m) {
        MetricPercentile metricPercentile = m.getPercentile();
        String field = metricPercentile.getField();
        Double margin = metricPercentile.getMargin();

        if (percentileTables == null) {
            percentileTables = new HashMap<>();
        }

        // Every metric will have only one value in the future.
        com.zoomdata.edc.server.core.sql.Percentile percentile = new com.zoomdata.edc.server.core.sql.Percentile(field, margin);

        // Will return something only if two metrics are equal.
        // Otherwise each metric will have own subquery.
        RelationalPath<?> percentileTable = percentileTables.get(percentile);
        if (percentileTable == null) {
            percentileTable = createTable(getPercentileSubqueryAliasPrefix() + (percentileTables.size() + 1));
            percentileTables.put(percentile, percentileTable);
        }

        return numberPath(Double.class, percentileTable, field).min()
                .as(getPercentileFieldAliasPrefix() + SQLUtils.underscore(String.valueOf(percentile.getPercentile())) +
                        "_over_" + SQLUtils.underscore(field));
    }

    @SuppressWarnings("unchecked")
    protected void joinLastValueTables(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                                       com.zoomdata.edc.server.core.sql.FiltersProcessor filtersProcessor, GroupsProcessor groupsProcessor) {
        List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> groupBy = groupsProcessor.getGroupBy();
        List<Group> groups = groupsProcessor.getThriftGroups();
        List<Filter> filters = filtersProcessor.getThriftFilters();

        for (Map.Entry<String, RelationalPath<?>> entry : lastValueTables.entrySet()) {
            String timeField = entry.getKey();
            RelationalPath<?> subqueryTable = entry.getValue();

            DateTimeExpression<Date> maxTimeFieldExpression = dateTimePath(Date.class, subqueryTable, timeField).max()
                    .as(getLastValueMaxTimeFieldAliasPrefix() + timeField);

            Predicate subqueryWhere = (filters == null) ? null :
                    sqlQueryBuilder.createFiltersProcessor().process(subqueryTable, filters);

            if (groupBy != null) {
                // Same fields in GROUP BY, but by internal table.
                List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> subqueryGroupBy =
                        sqlQueryBuilder.createGroupsProcessor().process(subqueryTable, groups, Collections.emptyMap());

                // SELECT contains all group fields and and MAX(<timeField>).
                Expression[] subquerySelect = SQLUtils.combine(subqueryGroupBy, maxTimeFieldExpression);


                // Join by all GROUP BY fields.
                BooleanBuilder subqueryOn = SQLUtils.createFieldsEqualPredicate(groupBy, subqueryGroupBy, subqueryTable);

                sqlQuery.leftJoin(
                        SQLExpressions
                                .select(subquerySelect)
                                .from(subqueryTable)
                                .where(subqueryWhere)
                                .groupBy(SQLUtils.toArray(Expression.class, subqueryGroupBy)),
                        subqueryTable)
                        .on(subqueryOn);
            } else {
                // Just join MAX(<timeField>) to every row of the main table to make CASE() in SELECT in the same
                // manner as when we have groups.
                sqlQuery.leftJoin(
                        SQLExpressions
                                .select(maxTimeFieldExpression)
                                .from(subqueryTable)
                                .where(subqueryWhere),
                        subqueryTable)
                        .on(TRUE);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void joinPercentileTables(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                                        com.zoomdata.edc.server.core.sql.FiltersProcessor filtersProcessor, GroupsProcessor groupsProcessor) {
        List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> groupBy = groupsProcessor.getGroupBy();
        List<Group> groups = groupsProcessor.getThriftGroups();
        List<Filter> filters = filtersProcessor.getThriftFilters();

        for (Map.Entry<com.zoomdata.edc.server.core.sql.Percentile, RelationalPath<?>> entry : percentileTables.entrySet()) {
            com.zoomdata.edc.server.core.sql.Percentile p = entry.getKey();

            String field = p.getField();
            double percentile = p.getPercentile();

            RelationalPath<?> percentileTable = entry.getValue();
            String percentileTableAlias = (String) percentileTable.getMetadata().getElement();

            NumberPath<Double> fieldExpression = numberPath(Double.class, percentileTable, field);
            Predicate percentileTableWhere = (filters == null) ? null :
                    sqlQueryBuilder.createFiltersProcessor().process(percentileTable, filters);

            // Agg.
            RelationalPath<?> aggTable = createTable(percentileTableAlias + "_agg");
            NumberPath<Double> fieldExpressionInAgg = numberPath(Double.class, aggTable, field);
            Predicate aggTableWhere = (filters == null) ? null :
                    sqlQueryBuilder.createFiltersProcessor().process(aggTable, filters);


            // Total.
            RelationalPath<?> totalTable = createTable(percentileTableAlias + "_t");
            NumberExpression<Long> totalTableCount = Wildcard.count.as("total");
            Predicate totalTableWhere = (filters == null) ? null :
                    sqlQueryBuilder.createFiltersProcessor().process(totalTable, filters);

            if (groupBy != null) {
                List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> percentileTableGroupBy =
                        sqlQueryBuilder.createGroupsProcessor().process(percentileTable, groups, Collections.emptyMap());

                // Agg subquery.
                List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> groupExpressionsInAgg =
                        sqlQueryBuilder.createGroupsProcessor().process(aggTable, groups, Collections.emptyMap());


                SQLQuery aggSubquery = SQLExpressions
                        .select(SQLUtils.combine(groupExpressionsInAgg, fieldExpressionInAgg))
                        .from(aggTable)
                        .where(aggTableWhere);

                // Join by all GROUP BY fields and percentile.field >= agg.field.
                BooleanBuilder aggOn = SQLUtils.createFieldsEqualPredicate(
                        percentileTableGroupBy,
                        groupExpressionsInAgg,
                        aggTable
                );
                aggOn.and(fieldExpression.goe(fieldExpressionInAgg));


                // Total subquery.
                List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> groupExpressionsInTotal =
                        sqlQueryBuilder.createGroupsProcessor().process(totalTable, groups, Collections.emptyMap());

                SQLQuery totalSubquery = SQLExpressions
                        .select(SQLUtils.combine(groupExpressionsInTotal, totalTableCount))
                        .from(totalTable)
                        .where(totalTableWhere)
                        .groupBy(SQLUtils.combine(groupExpressionsInTotal));

                // Join by all GROUP BY fields.
                BooleanBuilder totalOn = SQLUtils.createFieldsEqualPredicate(
                        percentileTableGroupBy,
                        groupExpressionsInTotal,
                        totalTable
                );

                // Join by all GROUP BY fields.
                Predicate percentileTableOn =
                        SQLUtils.createFieldsEqualPredicate(
                                groupBy,
                                percentileTableGroupBy,
                                percentileTable
                        );

                BooleanExpression percentileTableHaving = numberPath(Double.class, aggTable, field).count()
                        .multiply(numberTemplate(Integer.class, "100"))
                        .divide(numberPath(Integer.class, totalTable, "total").max())
                        .goe(numberTemplate(Double.class, String.valueOf(percentile)));

                Expression[] groupAndPercentileFields =
                        SQLUtils.combine(percentileTableGroupBy, fieldExpression);

                sqlQuery.innerJoin(
                        SQLExpressions
                                .select(groupAndPercentileFields)
                                .from(percentileTable)
                                .innerJoin(aggSubquery, aggTable).on(aggOn)
                                .innerJoin(totalSubquery, totalTable).on(totalOn)
                                .where(percentileTableWhere)
                                .groupBy(groupAndPercentileFields)
                                .having(percentileTableHaving)
                        , percentileTable)
                        .on(percentileTableOn);
            } else {
                SQLQuery aggSubquery = SQLExpressions
                        .select(fieldExpressionInAgg)
                        .from(aggTable)
                        .where(aggTableWhere);

                Predicate aggOn = fieldExpression.goe(fieldExpressionInAgg);

                SQLQuery totalSubquery = SQLExpressions
                        .select(totalTableCount)
                        .from(totalTable)
                        .where(totalTableWhere);

                BooleanExpression percentileTableHaving = numberPath(Double.class, aggTable, field).count()
                        .multiply(numberTemplate(Integer.class, "100"))
                        .divide(numberPath(Integer.class, totalTable, "total").max())
                        .goe(numberTemplate(Double.class, String.valueOf(percentile)));

                sqlQuery.innerJoin(
                        SQLExpressions
                                .select(fieldExpression)
                                .from(percentileTable)
                                .innerJoin(aggSubquery, aggTable).on(aggOn)
                                .innerJoin(totalSubquery, totalTable).on(TRUE)
                                .where(percentileTableWhere)
                                .groupBy(fieldExpression)
                                .having(percentileTableHaving)
                        , percentileTable)
                        .on(TRUE);
            }
        }
    }

    protected String getLastValueMaxTimeFieldAliasPrefix() {
        return LAST_VALUE_MAX_TIME_FIELD_ALIAS_PREFIX;
    }

    protected String getLastValueSubqueryAliasPrefix() {
        return LAST_VALUE_SUBQUERY_ALIAS_PREFIX;
    }

    protected String getLastValueFieldAliasPrefix() {
        return LAST_VALUE_FIELD_ALIAS_PREFIX;
    }

    protected String getPercentileSubqueryAliasPrefix() {
        return PERCENTILE_SUBQUERY_ALIAS_PREFIX;
    }

    protected String getPercentileFieldAliasPrefix() {
        return PERCENTILE_FIELD_ALIAS_PREFIX;
    }
}
