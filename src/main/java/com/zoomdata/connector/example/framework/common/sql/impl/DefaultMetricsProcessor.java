/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.*;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.zoomdata.connector.example.common.utils.StringUtils;
import com.zoomdata.connector.example.framework.common.sql.FiltersProcessor;
import com.zoomdata.connector.example.framework.common.sql.GroupsProcessor;
import com.zoomdata.connector.example.framework.common.sql.MetricsProcessor;
import com.zoomdata.connector.example.framework.common.sql.SQLQueryBuilder;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.metric.*;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.*;

import static com.querydsl.core.types.dsl.Expressions.*;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.createPercentileRowNumberAliasName;

//import com.querydsl.core.types.dsl.*;


public class DefaultMetricsProcessor implements MetricsProcessor {

    public static final String ALIAS_NAME_TTLCNT = "ttlCntByGrp";
    public static final String LAST_VALUE_MAX_TIME_FIELD_ALIAS_PREFIX = "__max_";
    public static final String LAST_VALUE_SUBQUERY_ALIAS_PREFIX = "lv_";
    public static final String LAST_VALUE_FIELD_ALIAS_PREFIX = "__lv_";
    public static final String PERCENTILE_FIELD_ALIAS_PREFIX = "__prc_";

    protected Path<?> table;
    protected List<Metric> thriftMetrics;
    protected Map<Metric, ComparableExpressionBase> metricExpressions = new HashMap<>();
    protected Map<String, AliasedSimpleExpression<?>> lastValueTables;
    protected Map<String, FieldMetadata> fieldMetadata = new HashMap<>();

    List<ComparableExpressionBase> select;

    private AliasedSimpleExpression fromClause;

    public DefaultMetricsProcessor() {
    }

    public DefaultMetricsProcessor(Map<String, FieldMetadata> fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
    }

    public DefaultMetricsProcessor(Map<String, FieldMetadata> fieldMetadata, AliasedSimpleExpression fromClause) {
        this(fieldMetadata);
        this.fromClause = fromClause;
    }

    @Override
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

    @Override
    public void doJoins(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                        FiltersProcessor filtersProcessor, GroupsProcessor groupsProcessor) {
        if (lastValueTables != null) {
            joinLastValueTables(sqlQueryBuilder, sqlQuery, filtersProcessor, groupsProcessor);
        }
    }

    @Override
    public Path<?> getTable() {
        return table;
    }

    @Override
    public List<Metric> getThriftMetrics() {
        return thriftMetrics;
    }

    @Override
    public List<ComparableExpressionBase> getSelect() {
        return select;
    }

    @Override
    public ComparableExpressionBase getMetricExpression(Metric metric) {
        return metricExpressions.get(metric);
    }

    // ==================== METRICS IMPLEMENTATION ====================

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

        AliasedSimpleExpression<?> lastValueTable = lastValueTables.get(timeField);
        if (lastValueTable == null) {
            String lvTableAlias = getLastValueSubqueryAliasPrefix() + (lastValueTables.size() + 1);
            lastValueTable = fromClause.withNewAlias(lvTableAlias);
            lastValueTables.put(timeField, lastValueTable);
        }

        return cases()
            .when(dateTimePath(Date.class, table, timeField)
                .eq(dateTimePath(Date.class, lastValueTable.getPath(),
                    getLastValueMaxTimeFieldAliasPrefix() + timeField)))
            .then(stringPath(table, field))
            .otherwise(nullExpression())
            .max().as(getLastValueFieldAliasPrefix() + field + "_over_" +
                Utils.underscore(timeField));
    }

    protected ComparableExpressionBase processPERCENTILES(Metric m) {
        final MetricPercentile mPercentile = m.getPercentile();
        final NumberOperation<Double> percentilePath =
            Expressions.numberOperation(Double.class, Ops.MULT,
                Expressions.numberOperation(Double.class, Ops.DIV,
                    Expressions.numberPath(Long.class, table, createPercentileRowNumberAliasName(mPercentile)).doubleValue(),
                    Expressions.numberPath(Long.class, table, getTotalCountAlias()).doubleValue()),
                constant(100));
        return cases()
            .when(Expressions.booleanOperation(Ops.GOE, percentilePath, constant(mPercentile.getMargin())))
            .then(numberPath(Double.class, table, mPercentile.getField()))
            .otherwise(Expressions.nullExpression())
            .min();
    }

    @SuppressWarnings("unchecked")
    protected void joinLastValueTables(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                                       FiltersProcessor filtersProcessor, GroupsProcessor groupsProcessor) {
        List<AliasedComparableExpressionBase> groupBy = groupsProcessor.getGroupBy();
        List<Group> groups = groupsProcessor.getThriftGroups();
        List<Filter> filters = filtersProcessor.getThriftFilters();

        for (Map.Entry<String, AliasedSimpleExpression<?>> entry : lastValueTables.entrySet()) {
            String timeField = entry.getKey();
            AliasedSimpleExpression<?> subqueryTableDef = entry.getValue();
            Path<?> subqueryTableRef = subqueryTableDef.getPath();

            DateTimeExpression<Date> maxTimeFieldExpression = dateTimePath(Date.class, subqueryTableRef, timeField).max()
                .as(getLastValueMaxTimeFieldAliasPrefix() + timeField);

            Predicate subqueryWhere = (filters == null) ? null :
                sqlQueryBuilder.createFiltersProcessor().process(subqueryTableRef, filters);

            if (groupBy != null) {
                // Same fields in GROUP BY, but by internal table.
                List<AliasedComparableExpressionBase> subqueryGroupBy =
                    sqlQueryBuilder.createGroupsProcessor().process(subqueryTableRef, groups, fieldMetadata);

                // SELECT contains all group fields and and MAX(<timeField>).
                Expression[] subquerySelect = Utils.combine(subqueryGroupBy, maxTimeFieldExpression);


                // Join by all GROUP BY fields.
                BooleanBuilder subqueryOn = Utils.createFieldsEqualCoalescePredicate(groupBy, subqueryGroupBy, subqueryTableRef);

                sqlQuery.leftJoin(
                    SQLExpressions
                        .select(subquerySelect)
                        .from(subqueryTableDef)
                        .where(subqueryWhere)
                        .groupBy(Utils.toArray(Expression.class, getSubqueryGroupBy(subqueryGroupBy))),
                    subqueryTableRef)
                    .on(subqueryOn);
            } else {
                // Just join MAX(<timeField>) to every row of the main table to make CASE() in SELECT in the same
                // manner as when we have groups.
                sqlQuery.from(
                    SQLExpressions
                        .select(maxTimeFieldExpression)
                        .from(subqueryTableDef)
                        .where(subqueryWhere),
                    subqueryTableRef);
            }
        }
    }

    protected List<AliasedComparableExpressionBase> getSubqueryGroupBy(List<AliasedComparableExpressionBase> subqueryGroupBy) {
        return subqueryGroupBy;
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

    protected String getTotalCountAlias() {
        return ALIAS_NAME_TTLCNT;
    }

    protected String getPercentileFieldAliasPrefix() {
        return PERCENTILE_FIELD_ALIAS_PREFIX;
    }
}
