/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

import com.zoomdata.gen.edc.types.Record;
import com.zoomdata.gen.edc.types.ResponseMetadata;

import java.util.Iterator;
import java.util.List;

public interface Cursor extends Iterator<Record> {

    List<ResponseMetadata> getMetadata();

    boolean hasNextBatch();
}
