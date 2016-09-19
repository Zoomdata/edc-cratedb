/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters;

import com.zoomdata.gen.edc.request.serverdescription.ConnectionParameter;

public interface IConnectionParameter {
    ConnectionParameter toThrift();

    void validate(String value);

    String getName();
}
