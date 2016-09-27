/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

public class AsyncException extends RuntimeException {

    public AsyncException(String message) {
        super(message);
    }

    public AsyncException(Throwable cause) {
        super(cause);
    }

    public AsyncException(String message, Throwable cause) {
        super(message, cause);
    }
}
