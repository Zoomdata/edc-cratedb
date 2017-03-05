/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb;

import com.google.common.collect.ImmutableMap;
import com.zoomdata.connector.example.framework.api.IFeatures;

import java.util.Map;

// The list of features implemented by this connector
// Some "features" are actually more like flags for now. See official Zoomdata documentation for more details
public class CrateDBFeatures implements IFeatures {

    private final Map<String, String> features =
            ImmutableMap.<String, String>builder()
                    .put("REQUEST.SEND_METADATA", "true")
                    .put("FEATURE.LIVE_SOURCE", "true")
                    .put("FEATURE.FAST_DISTINCT_VALUES", "true")
                    .put("FEATURE.GROUP_BY_TIME", "true")
                    .put("FEATURE.GROUP_BY_TIME.GROUP_BY_UNIX_TIME", "true")
                    .put("FEATURE.MULTI_GROUP_SUPPORT", "true")
                    .put("FEATURE.REFRESHABLE", "true")
                    .put("FEATURE.SUPPORT_OPTIMIZED_READ", "false")
                    .put("FEATURE.TEXT_SEARCH", "false")
                    .put("FEATURE.RAW_DATA_ONLY", "false")
                    .put("FEATURE.OFFSET", "true")
                    .put("FEATURE.DISTINCT_COUNT", "true")
                    .put("FEATURE.DISTINCT_COUNT.DISTINCT_COUNT_ONLY_ONE", "false")
                    .put("FEATURE.PARTITION", "false")
                    .put("FEATURE.SUPPORTS_MULTI_VALUED", "false")
                    .put("FEATURE.SUPPORTS_NESTED", "false")
                    .put("FEATURE.SUPPORTED_BY_SPARKIT", "true")
                    .put("FEATURE.SUPPORTS_SCHEMA", "true")
                    // Supported since Crate 1.0.3
                    // See https://github.com/crate/crate/issues/4673
                    .put("FEATURE.CUSTOM_QUERY", "true")
                    // Not a real "feature" but this is required for enabling FEATURE.PAGING_AND_SORTING.AGGREGATED
                    .put("FEATURE.PAGING_AND_SORTING", "true")
                    .put("FEATURE.PAGING_AND_SORTING.AGGREGATED", "true")
                    // Not a real "feature" but this is required for Zoomdata to communicate
                    .put("REQUEST.TYPE", "STRUCTURED")
                    // Crate doesn't support the necessary syntax for these features
                    .put("FEATURE.LV_METRIC", "false")
                    .put("FEATURE.PERCENTILES", "false")
                    .put("FEATURE.HISTOGRAM", "false")
                    .build();

    @Override
    public Map<String, String> getAllFeatures() {
        return features;
    }
}
