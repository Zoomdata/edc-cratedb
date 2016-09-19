/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

public interface IComputeTask {

    Cursor compute();

    double progress();

    void cancel();

    void close();
}
