/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils.metadatabuilders;

import com.zoomdata.gen.edc.request.ResponseInfo;
import com.zoomdata.gen.edc.request.ResponseStatus;

public class ResponseInfoBuilder {
    public static ResponseInfo success(String message) {
        return new ResponseInfo(ResponseStatus.SUCCESS, message);
    }

    public static ResponseInfo serverError(String message) {
        return new ResponseInfo(ResponseStatus.SERVER_ERROR, message);
    }
}
