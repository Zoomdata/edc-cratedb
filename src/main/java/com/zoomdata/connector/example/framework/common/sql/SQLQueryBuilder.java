/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Path;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.metric.Metric;
import com.zoomdata.gen.edc.request.StatField;
import com.zoomdata.gen.edc.sort.AggSort;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SQLQueryBuilder builds SQL queries!
 *
 * This interface consists of who parts. API methods is used to build SQL query. Factory methods create processors that
 * implement logic to build different part of SQL query.
 */
public interface SQLQueryBuilder {

    /**
     *
     * @param schemaName db schema, used only for isCustomSql false
     * @param tableName db table name or customSql query
     * @param isCustomSql identify tablename content
     * @param fieldMetadata metaData of fields (@see FieldMetadata)
     * @return @see SQLQueryBuilder
     */
    SQLQueryBuilder init(String schemaName, String tableName, Boolean isCustomSql, Map<String, FieldMetadata> fieldMetadata);

    // ==================== API METHODS ====================

    SQLQueryBuilder withFields(List<String> fields);

    SQLQueryBuilder withFilters(List<Filter> filters);

    SQLQueryBuilder withMetrics(List<Metric> metrics);

    SQLQueryBuilder withStats(List<StatField> stats);

    SQLQueryBuilder withRawSorts(List<RawSort> sorts);

    SQLQueryBuilder withAggSorts(List<AggSort> sorts);

    SQLQueryBuilder withGroups(List<Group> groups);

    SQLQueryBuilder withOffset(int offset);

    SQLQueryBuilder withLimit(int limit);

    SQLQueryBuilder withServerTimeZone(Optional<String> serverTimeZone);

    SQLQueryBuilder distinct();

    SQLQueryBuilder distinct(boolean distinct);

    ParametrizedQuery build(SQLTemplates templates);

    // ==================== FACTORIES OF PROCESSORS ====================

    FieldsProcessor createFieldsProcessor();

    MetricsProcessor createMetricProcessor();

    StatsProcessor createStatsProcessor();

    FiltersProcessor createFiltersProcessor(Path<?> table);

    IGroupExpressionProducer createGroupsProcessor();

    RawSortsProcessor createRawSortsProcessor();

    AggSortsProcessor createAggSortsProcessor();
}
