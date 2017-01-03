/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.google.common.base.Supplier;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberOperation;
import com.querydsl.core.types.dsl.StringExpression;
import com.querydsl.core.types.dsl.Wildcard;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.zoomdata.connector.example.common.utils.StringUtils;
import com.zoomdata.connector.example.framework.common.sql.IAliasGenerator;
import com.zoomdata.connector.example.framework.common.sql.IGroupExpressionProducer;
import com.zoomdata.connector.example.framework.common.sql.MetricsProcessor;
import com.zoomdata.connector.example.framework.common.sql.SQLQueryBuilder;
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
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.querydsl.core.types.dsl.Expressions.cases;
import static com.querydsl.core.types.dsl.Expressions.comparablePath;
import static com.querydsl.core.types.dsl.Expressions.constant;
import static com.querydsl.core.types.dsl.Expressions.dateTimePath;
import static com.querydsl.core.types.dsl.Expressions.nullExpression;
import static com.querydsl.core.types.dsl.Expressions.numberPath;
import static com.querydsl.core.types.dsl.Expressions.stringPath;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
    protected Map<String, FieldMetadata> fieldMetadata;

    // FIXME This collections contains the same data as in metricExpressions, so there is unreasonable data duplication
    List<ComparableExpressionBase> select;

    private AliasedSimpleExpression fromClause;

    protected IAliasGenerator aliasGenerator;
    private Supplier<AliasGenerator> aliasGeneratorSupplier = () -> AliasGenerator.newAliasGenerator().build();

    public DefaultMetricsProcessor() {
        this(null);
    }

    public DefaultMetricsProcessor(Map<String, FieldMetadata> fieldMetadata) {
        this(fieldMetadata, null);
    }

    public DefaultMetricsProcessor(Map<String, FieldMetadata> fieldMetadata, AliasedSimpleExpression fromClause) {
        this.fieldMetadata = Optional.ofNullable(fieldMetadata).orElse(new HashMap<>());
        this.fromClause = fromClause;
    }

    public DefaultMetricsProcessor useAliasGeneratorSupplier(Supplier<AliasGenerator> supplier) {
        this.aliasGeneratorSupplier = supplier;
        return this;
    }

    @Override
    public List<ComparableExpressionBase> process(Path<?> table, List<Metric> metrics) {
        aliasGenerator = aliasGeneratorSupplier.get();
        this.table = table;
        this.thriftMetrics = metrics;
        select = new ArrayList<>(metrics.size());
        for (Metric metric : metrics) {
            ComparableExpressionBase expression = process(metric);
            select.add(expression);
            metricExpressions.put(metric, expression);
        }
        return select;
    }

    ComparableExpressionBase process(Metric metric) {
        switch (metric.getType()) {
            case COUNT: {
                return processCOUNT(metric);
            }
            case MIN: {
                return processMIN(metric);
            }
            case MAX: {
                return processMAX(metric);
            }
            case AVG: {
                return processAVG(metric);
            }
            case SUM: {
                return processSUM(metric);
            }
            case DISTINCT_COUNT: {
                return processDISTINCT_COUNT(metric);
            }
            case LAST_VALUE: {
                return processLAST_VALUE(metric);
            }
            case PERCENTILES: {
                return processPERCENTILES(metric);
            }
            default: {
                throw new IllegalStateException("Metric of type " + metric.getType() + " is not supported.");
            }
        }
    }

    @Override
    public void doJoins(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                        List<Filter> thriftFilters, IGroupExpressionProducer groupsProcessor) {
        if (lastValueTables != null) {
            joinLastValueTables(sqlQueryBuilder, sqlQuery, thriftFilters, groupsProcessor);
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


    protected String createPercentileRowNumberAliasName(MetricPercentile percentile) {
        return createPercentileRowNumberAliasName(percentile.getField());
    }

    @Override
    public String createPercentileRowNumberAliasName(String percentileField) {
        return aliasGenerator.generateForPercentiles(getPercentileFieldAliasPrefix() + percentileField + "_rn");
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
                .max();
    }

    @SuppressWarnings("unchecked")
    protected void joinLastValueTables(SQLQueryBuilder sqlQueryBuilder, SQLQuery sqlQuery,
                                       List<Filter> thriftFilters, IGroupExpressionProducer groupsProcessor) {
        List<AliasedComparableExpressionBase> groupBy = groupsProcessor.getExpressionsForGroupBy();
        List<Group> groups = groupsProcessor.getThriftGroups();

        for (Map.Entry<String, AliasedSimpleExpression<?>> entry : lastValueTables.entrySet()) {
            String timeField = entry.getKey();
            AliasedSimpleExpression<?> subqueryTableDef = entry.getValue();
            Path<?> subqueryTableRef = subqueryTableDef.getPath();

            DateTimeExpression<Date> maxTimeFieldExpression = dateTimePath(Date.class, subqueryTableRef, timeField)
                    .max()
                    .as(getLastValueMaxTimeFieldAliasPrefix() + timeField);

            Predicate subqueryWhere = (thriftFilters == null) ? null :
                    sqlQueryBuilder.createFiltersProcessor(subqueryTableRef).process(thriftFilters);

            if (groupBy != null) {
                // Same fields in GROUP BY, but by internal table.

                IGroupExpressionProducer subqueryGroupsProcessor = sqlQueryBuilder.createGroupsProcessor()
                        .process(subqueryTableRef, groups, fieldMetadata);

                // SELECT contains all group fields and and MAX(<timeField>).
                Expression[] subquerySelect = Utils.combine(
                        subqueryGroupsProcessor.getExpressionsForSelect(),
                        maxTimeFieldExpression
                );

                // Join by all GROUP BY fields.
                BooleanBuilder joinPredicate = buildLastValueHelpTableJoinPredicate(
                        groupBy, subqueryGroupsProcessor.getExpressionsForSelect(), subqueryTableRef
                );

                sqlQuery.leftJoin(
                        SQLExpressions
                                .select(subquerySelect)
                                .from(subqueryTableDef)
                                .where(subqueryWhere)
                                .groupBy(Utils.toArray(Expression.class, subqueryGroupsProcessor.getExpressionsForGroupBy())),
                        subqueryTableRef
                ).on(joinPredicate);
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

    protected BooleanBuilder buildLastValueHelpTableJoinPredicate(List<AliasedComparableExpressionBase> mainTableGroupBy,
                                                                  List<AliasedComparableExpressionBase> helpTableGroupBy,
                                                                  Path<?> helpTableRef) {
        if (mainTableGroupBy.size() != helpTableGroupBy.size()) {
            throw new IllegalArgumentException(format("Failed to build join predicate for Last Value help table due " +
                    "to different number of expressions: %s vs %s", mainTableGroupBy, helpTableGroupBy));
        }

        List<StringExpression> leftExpressions = mainTableGroupBy.stream()
                .map(AliasedComparableExpressionBase::original)
                .map(this::toCoalesceExpression)
                .collect(toList());
        List<StringExpression> rightExpressions = helpTableGroupBy.stream()
                .map(expression -> expression.hasAlias() ?
                        comparablePath(expression.getType(), helpTableRef, expression.getAliasName()) : expression.original())
                .map(this::toCoalesceExpression)
                .collect(toList());

        BooleanBuilder predicate = new BooleanBuilder();
        Iterator<StringExpression> leftCoalesces = leftExpressions.iterator();
        Iterator<StringExpression> rightCoalesces = rightExpressions.iterator();
        while (leftCoalesces.hasNext()) {
            predicate.and(leftCoalesces.next().eq(rightCoalesces.next()));
        }
        return predicate;
    }

    protected StringExpression toCoalesceExpression(ComparableExpressionBase<?> expression) {
        return expression.coalesce(coalescePlaceholderFor(expression)).asString();
    }

    @SuppressWarnings("unchecked")
    protected Expression<?> coalescePlaceholderFor(ComparableExpressionBase<?> expression) {
        if (Number.class.isAssignableFrom(expression.getType())) {
            return Expressions.constant(987654321);
        } else if (Date.class.isAssignableFrom(expression.getType())) {
            return Expressions.constant(new Date(9876543210123L));
        }
        return Expressions.constant("COALESCE_987654321_PLACEHOLDER");
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
