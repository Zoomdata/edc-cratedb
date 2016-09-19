/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.google.common.collect.ImmutableList;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.SimpleExpression;
import com.querydsl.core.types.dsl.SimplePath;
import com.querydsl.core.types.dsl.SimpleTemplate;
import com.querydsl.sql.*;
import com.zoomdata.connector.example.framework.common.sql.*;
import com.zoomdata.connector.example.framework.common.sql.types.DateTimeWithMillisPrecision;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterFunction;
import com.zoomdata.gen.edc.filter.FilterISNULL;
import com.zoomdata.gen.edc.filter.FilterNOT;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.metric.Metric;
import com.zoomdata.gen.edc.request.StatField;
import com.zoomdata.gen.edc.sort.AggSort;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.types.FieldMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.querydsl.core.types.dsl.Expressions.simplePath;
import static com.querydsl.core.types.dsl.Expressions.stringPath;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.createPercentileRowNumberAliasName;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Builds SQL query for different RDBMS using QueryDSL library.
 */
public class DefaultSQLQueryBuilder implements SQLQueryBuilder {

    @SuppressWarnings("checkstyle:constantname")
    private static final Logger log = LoggerFactory.getLogger(DefaultSQLQueryBuilder.class);
    public static final String TABLE_ALIAS = "ds";
    protected String schemaName;
    protected String tableName;

    protected AliasedSimpleExpression<String> fromClause;
    protected Map<String, FieldMetadata> fieldMetadata;

    protected List<String> thriftFields;
    protected List<Metric> thriftMetrics;
    protected List<StatField> thriftStats;
    protected List<Filter> thriftFilters;
    protected List<Group> thriftGroups;
    protected List<RawSort> thriftRawSorts;
    protected List<AggSort> thriftAggSorts;

    protected boolean distinct = false;
    protected Integer offset;
    protected Integer limit;

    protected Optional<String> serverTimeZone = Optional.empty();

    @Override
    public SQLQueryBuilder init(String schemaName, String tableName, Boolean isCustomSql,
                                Map<String, FieldMetadata> fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fromClause = createFromClause(schemaName, tableName, isCustomSql);
        return this;
    }

    @SuppressWarnings("unchecked")
    protected AliasedSimpleExpression<String> createFromClause(String schemaName, String tableName, Boolean isCustomSql) {
        if (isCustomSql) {
            String customSql = tableName;
            SimpleTemplate<String> fromExpression = Expressions.template(String.class, "("+customSql+")");
            return AliasedSimpleExpression.create(fromExpression, TABLE_ALIAS);
        } else {
            SimplePath schema = simplePath(Object.class, schemaName);
            SimplePath table = simplePath(Object.class, schema, tableName);
            return AliasedSimpleExpression.create(table, TABLE_ALIAS);
        }
    }

    @Override
    public SQLQueryBuilder withFields(List<String> fields) {
        if (fields != null && !fields.isEmpty()) {
            if (thriftFields == null) {
                thriftFields = new ArrayList<>();
            }
            thriftFields.addAll(fields);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withMetrics(List<Metric> metrics) {
        if (metrics != null && !metrics.isEmpty()) {
            if (thriftMetrics == null) {
                thriftMetrics = new ArrayList<>();
            }
            thriftMetrics.addAll(metrics);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withStats(List<StatField> stats) {
        if (stats != null && !stats.isEmpty()) {
            if (thriftStats == null) {
                thriftStats = new ArrayList<>();
            }
            thriftStats.addAll(stats);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withFilters(List<Filter> filters) {
        if (filters != null && !filters.isEmpty()) {
            if (thriftFilters == null) {
                thriftFilters = new ArrayList<>();
            }
            thriftFilters.addAll(filters);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withGroups(List<Group> groups) {
        if (groups != null && !groups.isEmpty()) {
            if (thriftGroups == null) {
                thriftGroups = new ArrayList<>();
            }
            thriftGroups.addAll(groups);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withRawSorts(List<RawSort> sorts) {
        if (sorts != null && !sorts.isEmpty()) {
            if (thriftRawSorts == null) {
                thriftRawSorts = new ArrayList<>();
            }
            thriftRawSorts.addAll(sorts);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withAggSorts(List<AggSort> sorts) {
        if (sorts != null && !sorts.isEmpty()) {
            if (thriftAggSorts == null) {
                thriftAggSorts = new ArrayList<>();
            }
            thriftAggSorts.addAll(sorts);
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withOffset(int offset) {
        if (offset > 0) {
            this.offset = offset;
        }
        return this;
    }

    @Override
    public SQLQueryBuilder withLimit(int limit) {
        if (limit > 0) {
            this.limit = limit;
        }
        return this;
    }

    @Override
    public SQLQueryBuilder distinct() {
        return distinct(true);
    }

    @Override
    public DefaultSQLQueryBuilder distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    @Override
    public SQLQueryBuilder withServerTimeZone(Optional<String> serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ParametrizedQuery build(SQLTemplates templates) {
        validate();

        FieldsProcessor fieldsProcessor = createFieldsProcessor();
        MetricsProcessor metricProcessor = createMetricProcessor();
        StatsProcessor statsProcessor = createStatsProcessor();
        FiltersProcessor filtersProcessor = createFiltersProcessor();
        GroupsProcessor groupsProcessor = createGroupsProcessor();
        RawSortsProcessor rawSortsProcessor = createRawSortsProcessor();
        AggSortsProcessor aggSortsProcessor = createAggSortsProcessor();

        if (thriftFields != null) {
            fieldsProcessor.process(fromClause.getPath(), thriftFields);
        }
        if (thriftMetrics != null) {
            metricProcessor.process(fromClause.getPath(), thriftMetrics);
        }
        if (thriftGroups != null) {
            groupsProcessor.process(fromClause.getPath(), thriftGroups, fieldMetadata);
        }
        if (thriftStats != null) {
            statsProcessor.process(fromClause.getPath(), thriftStats);
        }


        Configuration config = new Configuration(templates);
        config.register(new DateTimeWithMillisPrecision());
        config.setUseLiterals(true);
        SQLQuery sqlQuery = new SQLQuery(config);

        Expression[] select = Utils.combineLists(
                groupsProcessor.getGroupBy(),
                fieldsProcessor.getSelect(),
                metricProcessor.getSelect(),
                statsProcessor.getSelect()
        );

        sqlQuery.select(select);
        if (useSubSelectForPercentiles()) {
            sqlQuery.from(
                createSubSelectForPercentile(templates, groupsProcessor.getGroupBy()),
                stringPath(TABLE_ALIAS)
            );
        } else {
            sqlQuery.from(fromClause);
        }
        if (thriftFilters != null) {
            filtersProcessor.process(fromClause.getPath(), thriftFilters);
        }
        sqlQuery.where(filtersProcessor.getWhere());

        metricProcessor.doJoins(this, sqlQuery, filtersProcessor, groupsProcessor);

        if (thriftGroups != null) {
            sqlQuery.groupBy(Utils.toArray(Expression.class, groupsProcessor.getGroupBy()));
        }

        if (thriftRawSorts != null || thriftAggSorts != null) {
            List<OrderSpecifier> orderBy = null;

            if (thriftRawSorts != null) {
                orderBy = rawSortsProcessor.process(fromClause.getPath(), thriftRawSorts);
            }

            if (thriftAggSorts != null) {
                orderBy = aggSortsProcessor.process(fromClause.getPath(), thriftAggSorts, metricProcessor, groupsProcessor);
            }

            sqlQuery.orderBy(Utils.toArray(OrderSpecifier.class, orderBy));
        }

        if (offset != null) {
            sqlQuery.offset(offset);
        }

        if (limit != null) {
            sqlQuery.limit(limit);
        }

        if (distinct) {
            sqlQuery.distinct();
        }

        SQLBindings sqlBindings = sqlQuery.getSQL();

        String sql = sqlBindings.getSQL();
        ImmutableList<Object> params = sqlBindings.getBindings();

        log.debug("sql " + sql);
        return new ParametrizedQuery(sql, params);
    }

    protected boolean useSubSelectForPercentiles() {
        return Optional.ofNullable(thriftMetrics)
            .map(list -> !list.isEmpty() && list.stream().anyMatch(Metric::isSetPercentile))
            .orElse(false);
    }

    protected ProjectableSQLQuery createSubSelectForPercentile(SQLTemplates templates,
                                                            List<AliasedComparableExpressionBase> groupBy) {
        final List<AliasedComparableExpressionBase> groupByWithOutAlias =
            Optional.ofNullable(groupBy)
            .map(l -> l.stream().map(AliasedComparableExpressionBase::withoutAlias).collect(toList()))
            .orElse(emptyList());
        final List<SimpleExpression<Long>> percentileWindowFunctions = thriftMetrics.stream()
            .filter(Metric::isSetPercentile)
            .map(Metric::getPercentile)
            .distinct()
            .map(percentile -> SQLExpressions.rowNumber()
                .over()
                .partitionBy(Utils.toArray(Expression.class, groupByWithOutAlias))
                .orderBy(stringPath(percentile.getField()))
                .as(createPercentileRowNumberAliasName(String.valueOf(percentile.getMargin()))))
            .collect(toList());
        percentileWindowFunctions.add(SQLExpressions.count().over()
            .partitionBy(Utils.toArray(Expression.class, groupByWithOutAlias))
            .as(DefaultMetricsProcessor.ALIAS_NAME_TTLCNT));
        Configuration config = new Configuration(templates);
        config.setUseLiterals(true);
        SQLQuery sqlQuery = new SQLQuery(config);

        final ProjectableSQLQuery query = sqlQuery.select(
            Utils.combineLists(
                fieldMetadata.keySet().stream().map(name -> stringPath(name)).collect(toList()),
                percentileWindowFunctions))
            .from(fromClause);

        if (thriftFilters != null) {
            thriftFilters.addAll(createNotNullPercentileFilter());
            query.where(createFiltersProcessor().process(fromClause.getPath(), thriftFilters));
            thriftFilters = null;
        } else {
            query.where(createFiltersProcessor().process(fromClause.getPath(), createNotNullPercentileFilter()));
        }


        return query;
    }

    private List<Filter> createNotNullPercentileFilter() {
        return thriftMetrics.stream()
            .filter(Metric::isSetPercentile)
            .map(m -> m.getPercentile().getField())
            .distinct().map(perc -> new Filter(FilterFunction.NOT)
            .setFilterNOT(new FilterNOT(new Filter(FilterFunction.IS_NULL)
                .setFilterISNULL(new FilterISNULL(perc, fieldMetadata.get(perc).getType())
                )))).collect(toList());
    }

    protected void validate() {
        if (thriftFields == null && thriftMetrics == null && thriftStats == null && thriftGroups == null) {
            throw new IllegalStateException("At least one of fields, metrics, stats or groups must be defined.");
        }
        if (thriftRawSorts != null && thriftAggSorts != null) {
            throw new IllegalStateException("RawSort and AggSort can't be defined simultaneously.");
        }
        if (fromClause.getPath() == null) {
            throw new IllegalArgumentException("Alias to a from clause definition is required");
        }
    }

    @Override
    public FieldsProcessor createFieldsProcessor() {
        return new DefaultFieldsProcessor();
    }

    @Override
    public MetricsProcessor createMetricProcessor() {
        return new DefaultMetricsProcessor(fieldMetadata, fromClause);
    }

    @Override
    public StatsProcessor createStatsProcessor() {
        return new DefaultStatsProcessor();
    }

    @Override
    public FiltersProcessor createFiltersProcessor() {
        return new DefaultFiltersProcessor();
    }

    @Override
    public GroupsProcessor createGroupsProcessor() {
        return new DefaultGroupsProcessor(serverTimeZone);
    }

    @Override
    public RawSortsProcessor createRawSortsProcessor() {
        return new DefaultRawSortsProcessor();
    }

    @Override
    public AggSortsProcessor createAggSortsProcessor() {
        return new DefaultAggSortsProcessor();
    }
}
