/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.sql.SQLTemplates;
import com.zoomdata.edc.server.core.ParametrizedQuery;
import com.zoomdata.edc.server.core.sql.SQLQueryBuilder;
import com.zoomdata.gen.edc.request.AggDataRequest;
import com.zoomdata.gen.edc.request.DataReadRequest;
import com.zoomdata.gen.edc.request.DistinctValuesRequest;
import com.zoomdata.gen.edc.request.RawDataRequest;
import com.zoomdata.gen.edc.request.StatsDataRequest;
import com.zoomdata.gen.edc.request.StructuredRequest;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.Collections;
import java.util.Map;

public class StructuredToSQLTransformer {
    public static ParametrizedQuery transform(DataReadRequest request,
                                              com.zoomdata.edc.server.core.sql.SQLQueryBuilder sqlQueryBuilder, SQLTemplates templates) {
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
                                                 com.zoomdata.edc.server.core.sql.SQLQueryBuilder sqlQueryBuilder, SQLTemplates templates) {
        StructuredRequest structuredRequest = request.getStructured();

        final Map<String, FieldMetadata> fieldMetadata = structuredRequest.getFieldMetadata();
        String schema = structuredRequest.getCollectionInfo().getSchema();
        String collection = structuredRequest.getCollectionInfo().getCollection();

        switch (structuredRequest.getType()) {
            case RAW: {
                RawDataRequest dataRequest = structuredRequest.getRawDataRequest();
                return sqlQueryBuilder.init(schema, collection, fieldMetadata)
                        .withFields(dataRequest.getFields())
                        .withFilters(dataRequest.getFilters())
                        .withRawSorts(dataRequest.getSorts())
                        .withLimit(dataRequest.getLimit())
                        .withOffset(dataRequest.getOffset())
                        .build(templates);
            }

            case AGG: {
                AggDataRequest dataRequest = structuredRequest.getAggDataRequest();
                return sqlQueryBuilder.init(schema, collection, fieldMetadata)
                        .withGroups(dataRequest.getGroups())
                        .withMetrics(dataRequest.getMetrics())
                        .withAggSorts(dataRequest.getSorts())
                        .withFilters(dataRequest.getFilters())
                        .withLimit(dataRequest.getLimit())
                        .withOffset(dataRequest.getOffset())
                        .build(templates);
            }

            case STATS: {
                StatsDataRequest dataRequest = structuredRequest.getStatsDataRequest();
                return sqlQueryBuilder.init(schema, collection, fieldMetadata)
                        .withStats(dataRequest.getStatFields())
                        .build(templates);
            }

            case DISTINCT_VALUES: {
                DistinctValuesRequest dataRequest = structuredRequest.getDistinctValuesRequest();
                return sqlQueryBuilder.init(schema, collection, fieldMetadata)
                        .withFields(Collections.singletonList(dataRequest.getField())).distinct()
                        .withFilters(dataRequest.getFilters())
                                // Ascending sort by default.
                        .withRawSorts(Collections.singletonList(new RawSort(dataRequest.getField())))
                        .withLimit(dataRequest.getLimit())
                        .withOffset(dataRequest.getOffset())
                        .build(templates);
            }

            default: {
                throw new IllegalStateException("Unknown type of DataReadRequest: " + request.getType());
            }
        }
    }

    private StructuredToSQLTransformer() {
    }
}