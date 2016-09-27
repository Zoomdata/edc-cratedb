/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.sql.SQLTemplates;
import com.zoomdata.connector.example.framework.common.PropertiesExtractor;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.group.HistogramGroup;
import com.zoomdata.gen.edc.request.*;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.zoomdata.connector.example.common.utils.metadatabuilders.Filters.isNull;
import static com.zoomdata.connector.example.common.utils.metadatabuilders.Filters.not;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.isCustomSql;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;

/**
 * Utility class to transform {@link DataReadRequest} to SQL.

 */
public class StructuredToSQLTransformer {
    public static ParametrizedQuery transform(DataReadRequest request,
                                              SQLQueryBuilder sqlQueryBuilder, SQLTemplates templates) {
        switch (request.getType()) {
            case SQL: {
                return new ParametrizedQuery(request.getQuery());
            }
            case STRUCTURED: {
                return transformStructured(request, sqlQueryBuilder, templates);
            }
            default: {
                throw new IllegalStateException("Unknown type of DataReadRequest: " + request.getType());
            }
        }
    }

    static ParametrizedQuery transformStructured(DataReadRequest request,
                                                 SQLQueryBuilder sqlQueryBuilder, SQLTemplates templates) {
        StructuredRequest structuredRequest = request.getStructured();


        Boolean customSql = isCustomSql(structuredRequest.getCollectionInfo());
        final Map<String, FieldMetadata> fieldMetadata = structuredRequest.getFieldMetadata();
        String schema = structuredRequest.getCollectionInfo().getSchema();
        String collection = structuredRequest.getCollectionInfo().getCollection();
        Optional<String> serverTimeZone = PropertiesExtractor.extractServerTimeZone(request.getRequestInfo());

        switch (structuredRequest.getType()) {
            case RAW: {
                RawDataRequest dataRequest = structuredRequest.getRawDataRequest();
                return sqlQueryBuilder.init(schema, collection, customSql, fieldMetadata)
                        .withFields(dataRequest.getFields())
                        .withFilters(dataRequest.getFilters())
                        .withRawSorts(dataRequest.getSorts())
                        .withLimit(dataRequest.getLimit())
                        .withOffset(dataRequest.getOffset())
                        .withServerTimeZone(serverTimeZone)
                        .build(templates);
            }

            case AGG: {
                AggDataRequest dataRequest = structuredRequest.getAggDataRequest();
                List<Filter> filters = ofNullable(dataRequest.getFilters()).orElse(emptyList());

                dataRequest.getGroups().stream()
                        .filter(Group::isSetHistogramGroup)
                        .map(Group::getHistogramGroup)
                        .map(HistogramGroup::getField)
                        .map(fieldMetadata::get)
                        .map(field -> not(isNull(field.getName(), field.getType())))
                        .findAny()
                        .ifPresent(histogramFieldIsNotNull -> filters.add(histogramFieldIsNotNull));

                return sqlQueryBuilder.init(schema, collection, customSql, fieldMetadata)
                        .withGroups(dataRequest.getGroups())
                        .withMetrics(dataRequest.getMetrics())
                        .withAggSorts(dataRequest.getSorts())
                        .withFilters(filters)
                        .withLimit(dataRequest.getLimit())
                        .withOffset(dataRequest.getOffset())
                        .withServerTimeZone(serverTimeZone)
                        .build(templates);
            }

            case STATS: {
                StatsDataRequest dataRequest = structuredRequest.getStatsDataRequest();
                return sqlQueryBuilder.init(schema, collection, customSql, fieldMetadata)
                        .withStats(dataRequest.getStatFields())
                        .build(templates);
            }

            case DISTINCT_VALUES: {
                DistinctValuesRequest dataRequest = structuredRequest.getDistinctValuesRequest();
                String field = dataRequest.getField();
                FieldType type = fieldMetadata.get(field).getType();
                return sqlQueryBuilder.init(schema, collection, customSql, fieldMetadata)
                        .withFields(singletonList(field)).distinct()
                        .withFilters(dataRequest.getFilters())
                        // Always add IS NOT NULL filter.
                        .withFilters(singletonList(not(isNull(field, type))))
                        // Ascending sort by default.
                        .withRawSorts(getSortConfig(field, dataRequest.getSort()))
                        .withLimit(dataRequest.getLimit())
                        .withOffset(dataRequest.getOffset())
                        .withServerTimeZone(serverTimeZone)
                        .build(templates);
            }

            default: {
                throw new IllegalStateException("Unknown type of DataReadRequest: " + request.getType());
            }
        }
    }

    private static List<RawSort> getSortConfig(String field, RawSort sort) {
        return singletonList(sort == null ? new RawSort(field) : sort);
    }

    private StructuredToSQLTransformer() {
    }
}
