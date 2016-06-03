/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.cratedb;

import com.zoomdata.gen.edc.ConnectorConstants;

import java.util.HashMap;
import java.util.Map;

public class CrateDbInfo {

    // Advertise what features to be supported by this connector.
    // TODO: provide reference to docs that offer what vis are supported based on these features.
    public static final Map<String, String> INFO_PROPERTIES = new HashMap<String, String>() {{
        // driver version (for compatibility with datasource)
        put("DB.VERSION", "5.1.32");

        // api version
        put("API.VERSION", ConnectorConstants.VERSION);

        // request type which zoomdata will send, (STRUCTURED or SQL)
        // we recommend to use STRUCTURED and implement your own query builder
        // from StructuredRequest to sql string
        // SQL option is deprecated, but used here for simplicity

        //put("REQUEST.TYPE", "SQL");
        put("REQUEST.TYPE", "STRUCTURED");

        // if you are using SQL reuqest type, this is preffered dialect
        //put("REQUEST.SQL_DIALECT", "MYSQL");

        // if zoomdata will send metadata for request back to you, recommended to true
        put("REQUEST.SEND_METADATA", "true");

        // features for zoomdata

        // if LIVE mode is available for zoomdata
        put("FEATURE.LIVE_SOURCE", "true");

        // if caching enabled for this connector
        put("FEATURE.CACHING_ENABLED", "false");

        // true - if you want to get query for all distinct values
        // otherwise zoomdata will take values from preview
        put("FEATURE.FAST_DISTINCT_VALUES", "true");

        // if grouping by time allowed
        put("FEATURE.GROUP_BY_TIME", "true");

        // if multi groups allowed
        put("FEATURE.MULTI_GROUP_SUPPORT", "true");

        // if it's possible to edit already created source
        put("FEATURE.REFRESHABLE", "true");

        // deprecated, false
        put("FEATURE.SUPPORT_OPTIMIZED_READ", "false");

        // last value metric available
        put("FEATURE.LV_METRIC", "true");

        // text search feature like for search sources (solr, elastic)
        put("FEATURE.TEXT_SEARCH", "false");

        // percentiles and box plot visualisation is available
        put("FEATURE.PERCENTILES", "false");
        put("FEATURE.PERCENTILES.PERCENTILES_GLOBAL_GROUP", "false");

        // histogram group available
        put("FEATURE.HISTOGRAM", "false");
        put("FEATURE.HISTOGRAM.HISTOGRAM_FOR_FLOAT_POINT_VALUES", "false");

        // datasource doesn't allow grouping and can only return raw values (like redis)
        put("FEATURE.RAW_DATA_ONLY", "false");
        put("FEATURE.DEPENDS_ON_SPARK", "false");
        put("FEATURE.SUPPORTED_BY_SPARKIT", "false");

        // offset working in O(1) time
        put("FEATURE.OFFSET", "true");

        // distinct count metric on any attribute
        put("FEATURE.DISTINCT_COUNT", "true");

        // datasource support schemas
        put("FEATURE.SUPPORTS_SCHEMA", "true");

        // custom sql option is available
        put("FEATURE.CUSTOM_QUERY", "true");

    }};

    public static String get(String key) {
        return INFO_PROPERTIES.getOrDefault(key, "UNKNOWN");
    }
}