/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.google.common.collect.ImmutableList;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.RelationalPathBase;
import com.querydsl.sql.SQLBindings;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.edc.server.core.ParametrizedQuery;
import com.zoomdata.edc.server.core.sql.*;
import com.zoomdata.edc.server.core.sql.AggSortsProcessor;
import com.zoomdata.edc.server.core.sql.FieldsProcessor;
import com.zoomdata.edc.server.core.sql.FiltersProcessor;
import com.zoomdata.edc.server.core.sql.GroupsProcessor;
import com.zoomdata.edc.server.core.sql.MetricsProcessor;
import com.zoomdata.edc.server.core.sql.RawSortsProcessor;
import com.zoomdata.edc.server.core.utils.SQLUtils;
import com.zoomdata.edc.server.core.utils.StringUtils;
import com.zoomdata.gen.edc.filter.Filter;
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

public class SQLQueryBuilder {

    private static final Logger log = LoggerFactory.getLogger(com.zoomdata.edc.server.core.sql.SQLQueryBuilder.class);

    private String schemaName;
    private String tableName;

    private Path table;
    private Expression fromClause;
    private Map<String, FieldMetadata> fieldMetadata;

    private List<String> thriftFields;
    private List<Metric> thriftMetrics;
    private List<StatField> thriftStats;
    private List<Filter> thriftFilters;
    private List<Group> thriftGroups;
    private List<RawSort> thriftRawSorts;
    private List<AggSort> thriftAggSorts;

    private boolean distinct = false;
    private Integer offset;
    private Integer limit;

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder init(String schemaName, String tableName, Map<String, FieldMetadata> fieldMetadata) {
        this.fieldMetadata = fieldMetadata;
        this.schemaName = schemaName;
        this.tableName = tableName;
        if (StringUtils.isEmpty(schemaName) == false) {
            this.tableName = this.schemaName + "." + this.tableName;
        }
        table = new RelationalPathBase(Object.class, "ds", this.schemaName, this.tableName);
        fromClause = table;
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withFields(List<String> fields) {
        if (fields != null && !fields.isEmpty()) {
            if (thriftFields == null) {
                thriftFields = new ArrayList<>();
            }
            thriftFields.addAll(fields);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withMetrics(List<Metric> metrics) {
        if (metrics != null && !metrics.isEmpty()) {
            if (thriftMetrics == null) {
                thriftMetrics = new ArrayList<>();
            }
            thriftMetrics.addAll(metrics);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withStats(List<StatField> stats) {
        if (stats != null && !stats.isEmpty()) {
            if (thriftStats == null) {
                thriftStats = new ArrayList<>();
            }
            thriftStats.addAll(stats);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withFilters(List<Filter> filters) {
        if (filters != null && !filters.isEmpty()) {
            if (thriftFilters == null) {
                thriftFilters = new ArrayList<>();
            }
            thriftFilters.addAll(filters);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withGroups(List<Group> groups) {
        if (groups != null && !groups.isEmpty()) {
            if (thriftGroups == null) {
                thriftGroups = new ArrayList<>();
            }
            thriftGroups.addAll(groups);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withRawSorts(List<RawSort> sorts) {
        if (sorts != null && !sorts.isEmpty()) {
            if (thriftRawSorts == null) {
                thriftRawSorts = new ArrayList<>();
            }
            thriftRawSorts.addAll(sorts);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withAggSorts(List<AggSort> sorts) {
        if (sorts != null && !sorts.isEmpty()) {
            if (thriftAggSorts == null) {
                thriftAggSorts = new ArrayList<>();
            }
            thriftAggSorts.addAll(sorts);
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withOffset(int offset) {
        if (offset > 0) {
            this.offset = offset;
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder withLimit(int limit) {
        if (limit > 0) {
            this.limit = limit;
        }
        return this;
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder distinct() {
        return distinct(true);
    }

    public com.zoomdata.edc.server.core.sql.SQLQueryBuilder distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    @SuppressWarnings("unchecked")
    public ParametrizedQuery build(SQLTemplates templates) {
        validate(templates);

        com.zoomdata.edc.server.core.sql.FieldsProcessor fieldsProcessor = createFieldsProcessor();
        MetricsProcessor metricProcessor = createMetricProcessor();
        StatsProcessor statsProcessor = createStatsProcessor();
        FiltersProcessor filtersProcessor = createFiltersProcessor();

        GroupsProcessor groupsProcessor = createGroupsProcessor();
        RawSortsProcessor rawSortsProcessor = createRawSortsProcessor();
        AggSortsProcessor aggSortsProcessor = createAggSortsProcessor();

        if (thriftFields != null) {
            fieldsProcessor.process(table, thriftFields);
        }
        if (thriftMetrics != null) {
            metricProcessor.process(table, thriftMetrics);
        }
        if (thriftGroups != null) {
            groupsProcessor.process(table, thriftGroups, fieldMetadata);
        }
        if (thriftStats != null) {
            statsProcessor.process(table, thriftStats);
        }
        if (thriftFilters != null) {
            filtersProcessor.process(table, thriftFilters);
        }


        Configuration config = new Configuration(templates);
        config.setUseLiterals(true);
        SQLQuery sqlQuery = new SQLQuery(config);

        Expression[] select = SQLUtils.combineLists(
                groupsProcessor.getGroupBy(),
                fieldsProcessor.getSelect(),
                metricProcessor.getSelect(),
                statsProcessor.getSelect()
        );

        sqlQuery
                .select(select)
                .from(fromClause)
                .where(filtersProcessor.getWhere());

        metricProcessor.doJoins(this, sqlQuery, filtersProcessor, groupsProcessor);

        if (thriftGroups != null) {
            sqlQuery.groupBy(SQLUtils.toArray(Expression.class, groupsProcessor.getGroupBy()));
        }

        if (thriftRawSorts != null || thriftAggSorts != null) {
            List<OrderSpecifier> orderBy = null;

            if (thriftRawSorts != null) {
                orderBy = rawSortsProcessor.process(table, thriftRawSorts);
            }

            if (thriftAggSorts != null) {
                orderBy = aggSortsProcessor.process(table, thriftAggSorts, metricProcessor, groupsProcessor);
            }

            sqlQuery.orderBy(SQLUtils.toArray(OrderSpecifier.class, orderBy));
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

        if (log.isDebugEnabled()) {
            log.debug("sql " + sql);
        }
        return new ParametrizedQuery(sql, params);
    }

    protected void validate(SQLTemplates templates) {
        if (thriftFields == null && thriftMetrics == null && thriftStats == null && thriftGroups == null) {
            throw new IllegalStateException("At least one of fields, metrics, stats or groups must be defined.");
        }
        if (thriftRawSorts != null && thriftAggSorts != null) {
            throw new IllegalStateException("RawSort and AggSort can't be defined simultaneously.");
        }
        if (table instanceof RelationalPathBase && ((RelationalPathBase) table).getSchemaName() == null
                && templates.isPrintSchema()) {
            throw new IllegalArgumentException("Schema name is required");
        }
    }

    public com.zoomdata.edc.server.core.sql.FieldsProcessor createFieldsProcessor() {
        return new FieldsProcessor();
    }

    public MetricsProcessor createMetricProcessor() {
        return new MetricsProcessor();
    }

    public StatsProcessor createStatsProcessor() {
        return new StatsProcessor();
    }

    public FiltersProcessor createFiltersProcessor() {
        return new FiltersProcessor();
    }

    public GroupsProcessor createGroupsProcessor() {
        return new GroupsProcessor();
    }

    public RawSortsProcessor createRawSortsProcessor() {
        return new RawSortsProcessor();
    }

    public AggSortsProcessor createAggSortsProcessor() {
        return new AggSortsProcessor();
    }
}