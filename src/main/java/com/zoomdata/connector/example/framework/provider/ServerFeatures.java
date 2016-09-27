/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.google.common.collect.ImmutableMap;
import com.zoomdata.connector.example.framework.api.IFeatures;
import com.zoomdata.gen.edc.ConnectorConstants;

import java.util.Map;

public class ServerFeatures implements IFeatures {

    public static final String SERVER_VERSION = "VERSION";

    private final Map<String, String> features =
            ImmutableMap.<String, String>builder()
                    .put(SERVER_VERSION, ConnectorConstants.VERSION)
                    .build();

    @Override
    public Map<String, String> getAllFeatures() {
        return features;
    }
}
