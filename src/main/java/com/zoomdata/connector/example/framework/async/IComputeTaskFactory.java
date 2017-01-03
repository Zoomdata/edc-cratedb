/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

public interface IComputeTaskFactory {

    IComputeTask create();

    String getRawQuery();

    int getFetchSize();
}
