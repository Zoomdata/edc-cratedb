/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.api;

import com.zoomdata.gen.edc.request.RequestInfo;
import com.zoomdata.gen.edc.request.serverdescription.ServerDescription;

public interface IDescriptionProvider {

    void validate(RequestInfo requestInfo);

    ServerDescription describe();
}
