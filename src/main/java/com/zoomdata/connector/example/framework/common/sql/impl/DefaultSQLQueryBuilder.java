/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.google.common.collect.ImmutableList;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.SimplePath;
import com.querydsl.core.types.dsl.SimpleTemplate;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.ProjectableSQLQuery;
import com.querydsl.sql.SQLBindings;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.connector.example.common.utils.StructuredUtils;
import com.zoomdata.connector.example.common.utils.metadatabuilders.Filters;
import com.zoomdata.connector.example.framework.common.sql.AggSortsProcessor;
import com.zoomdata.connector.example.framework.common.sql.FieldsProcessor;
import com.zoomdata.connector.example.framework.common.sql.FiltersProcessor;
import com.zoomdata.connector.example.framework.common.sql.IGroupExpressionProducer;
import com.zoomdata.connector.example.framework.common.sql.MetricsProcessor;
import com.zoomdata.connector.example.framework.common.sql.ParametrizedQuery;
import com.zoomdata.connector.example.framework.common.sql.RawSortsProcessor;
import com.zoomdata.connector.example.framework.common.sql.SQLQueryBuilder;
import com.zoomdata.connector.example.framework.common.sql.StatsProcessor;
import com.zoomdata.connector.example.framework.common.sql.filter.type.DefaultFilterTypeService;
import com.zoomdata.connector.example.framework.common.sql.filter.type.FilterTypeService;
import com.zoomdata.connector.example.framework.common.sql.types.DateTimeWithMillisPrecision;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.metric.Metric;
import com.zoomdata.gen.edc.request.StatField;
import com.zoomdata.gen.edc.sort.AggSort;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.types.FieldMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.querydsl.core.types.dsl.Expressions.simplePath;
import static com.querydsl.core.types.dsl.Expressions.stringPath;
import static com.zoomdata.connector.example.common.utils.CollectionUtils.streamOfNullable;
import static java.util.Optional.ofNullable;
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

    private AliasedSimpleExpression<String> createFromClause(String schemaName, String tableName, Boolean isCustomSql) {
        return isCustomSql ? createFromClauseForCustomSql(tableName) : createFromClauseForTable(schemaName, tableName);
    }

    protected AliasedSimpleExpression<String> createFromClauseForCustomSql(String customSql) {
        SimpleTemplate<String> fromExpression = Expressions.template(String.class, "(" + customSql + ")");
        return AliasedSimpleExpression.create(fromExpression, TABLE_ALIAS);
    }

    protected AliasedSimpleExpression<String> createFromClauseForTable(String schemaName, String tableName) {
        SimplePath table;
        if (StringUtils.isEmpty(schemaName)) {
            table = simplePath(Object.class, tableName);
        } else {
            SimplePath schema = simplePath(Object.class, schemaName);
            table = simplePath(Object.class, schema, tableName);
        }
        return AliasedSimpleExpression.create(table, TABLE_ALIAS);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ParametrizedQuery build(SQLTemplates templates) {
        validate();

        FieldsProcessor fieldsProcessor = createFieldsProcessor();
        MetricsProcessor metricProcessor = createMetricProcessor();
        StatsProcessor statsProcessor = createStatsProcessor();
        FiltersProcessor filtersProcessor = createFiltersProcessor(fromClause.getPath());
        IGroupExpressionProducer groupsProcessor = createGroupsProcessor();
        RawSortsProcessor rawSortsProcessor = createRawSortsProcessor();
        AggSortsProcessor aggSortsProcessor = createAggSortsProcessor();

        // TODO create separate factory class for this purpose
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
                groupsProcessor.getExpressionsForSelect(),
                fieldsProcessor.getSelect(),
                metricProcessor.getSelect(),
                statsProcessor.getSelect()
        );

        sqlQuery.select(select);
        if (useSubSelectForPercentiles()) {
            sqlQuery.from(
                    createPercentileSubSelect(templates, metricProcessor, groupsProcessor),
                    stringPath(TABLE_ALIAS)
            );
        } else {
            sqlQuery.from(fromClause);
            if (thriftFilters != null) {
                filtersProcessor.process(thriftFilters);
            }
        }
        sqlQuery.where(filtersProcessor.getWhere());

        metricProcessor.doJoins(this, sqlQuery, thriftFilters, groupsProcessor);

        if (thriftGroups != null) {
            sqlQuery.groupBy(Utils.toArray(Expression.class, groupsProcessor.getExpressionsForGroupBy()));
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
            sqlQuery.limit(getSqlLimit(limit));
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

    protected long getSqlLimit(Integer limit) {
        return limit.longValue();
    }

    protected boolean useSubSelectForPercentiles() {
        return ofNullable(thriftMetrics)
                .map(list -> !list.isEmpty() && list.stream().anyMatch(Metric::isSetPercentile))
                .orElse(false);
    }

    private ProjectableSQLQuery createPercentileSubSelect(SQLTemplates templates,
                                                          MetricsProcessor metricsProcessor,
                                                          IGroupExpressionProducer groupsProcessor) {
        Configuration config = new Configuration(templates);
        config.setUseLiterals(true);

        Expression<?>[] projection =
                createPercentileSubSelectProjection(metricsProcessor, groupsProcessor).toArray(Expression<?>[]::new);

        Predicate predicate = createPercentileSubSelectPredicate();

        return new SQLQuery<Tuple>(config)
                .select(projection).from(fromClause)
                .where(predicate);
    }

    private Stream<Expression<?>> createPercentileSubSelectProjection(
            MetricsProcessor metricsProcessor, IGroupExpressionProducer groupsProcessor) {

        Expression<?>[] groupByExpressions =
                streamOfNullable(groupsProcessor.getExpressionsForGroupBy())
                        .map(AliasedComparableExpressionBase::withoutAlias)
                        .toArray(Expression<?>[]::new);

        Stream<Expression<?>> rowNumberExpressions = getPercentileFieldNames()
                .map(fieldName -> SQLExpressions.rowNumber().over()
                        .partitionBy(groupByExpressions)
                        .orderBy(stringPath(fieldName))
                        .as(metricsProcessor.createPercentileRowNumberAliasName(fieldName))
                );
        Expression<?> countExpression = SQLExpressions.count().over()
                .partitionBy(groupByExpressions)
                .as(DefaultMetricsProcessor.ALIAS_NAME_TTLCNT);

        Stream<Expression<?>> fieldPaths = Stream.concat(
                getMetricFieldNames(),
                getGroupFieldNames()
        ).distinct().map(Expressions::stringPath);

        Stream<Expression<?>> windowFunctions = Stream.concat(
                rowNumberExpressions,
                Stream.of(countExpression)
        );

        return Stream.concat(fieldPaths, windowFunctions);
    }

    private Predicate createPercentileSubSelectPredicate() {
        List<Filter> filters = Stream.concat(
                streamOfNullable(thriftFilters),
                getPercentileFieldNames().map(this::createNotNullFilter)
        ).collect(toList());
        return createFiltersProcessor(fromClause.getPath()).process(filters);
    }

    private Stream<String> getPercentileFieldNames() {
        return streamOfNullable(thriftMetrics)
                .filter(Metric::isSetPercentile)
                .map(metric -> metric.getPercentile().getField())
                .distinct();
    }

    private Stream<String> getMetricFieldNames() {
        return streamOfNullable(thriftMetrics)
                .map(StructuredUtils::getMetricName)
                .filter(Objects::nonNull)
                .distinct();
    }

    private Stream<String> getGroupFieldNames() {
        return streamOfNullable(thriftGroups)
                .map(StructuredUtils::getGroupName);
    }

    private Filter createNotNullFilter(String fieldName) {
        return Filters.not(Filters.isNull(fieldName, fieldMetadata.get(fieldName).getType()));
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

    // TODO create separate factory class for this purpose
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
    public FiltersProcessor createFiltersProcessor(Path<?> table) {
        return new DefaultFiltersProcessor(table, Optional.ofNullable(fieldMetadata).orElse(Collections.emptyMap()),
                createFilterTypeService());
    }

    @Override
    public IGroupExpressionProducer createGroupsProcessor() {
        return new GroupExpressionProducer();
    }

    @Override
    public RawSortsProcessor createRawSortsProcessor() {
        return new DefaultRawSortsProcessor();
    }

    @Override
    public AggSortsProcessor createAggSortsProcessor() {
        return new DefaultAggSortsProcessor();
    }

    protected FilterTypeService createFilterTypeService() {
        return new DefaultFilterTypeService();
    }

}
