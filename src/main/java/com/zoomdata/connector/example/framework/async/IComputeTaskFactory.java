/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

public interface IComputeTaskFactory {

    IComputeTask create();

    String getRawQuery();

    int getFetchSize();
}
