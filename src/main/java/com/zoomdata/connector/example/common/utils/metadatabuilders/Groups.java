/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.common.utils.metadatabuilders;

import com.zoomdata.gen.edc.group.AttributeGroup;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.group.GroupType;
import com.zoomdata.gen.edc.group.HistogramGroup;
import com.zoomdata.gen.edc.group.TimeGroup;
import com.zoomdata.gen.edc.types.TimeGranularity;

public final class Groups {

    private Groups() {
    }

    public static Group group(String field) {
        return new Group(GroupType.ATTRIBUTE_GROUP)
                .setAttributeGroup(new AttributeGroup()
                        .setField(field));
    }

    public static Group timeGroup(String field, TimeGranularity granularity) {
        return new Group(GroupType.TIME_GROUP).setTimeGroup(
                new TimeGroup()
                        .setGranularity(granularity)
                        .setField(field));
    }

    public static Group histogram(String field, double bucketSize) {
        return histogram(field, 0, 0, bucketSize);
    }

    public static Group histogram(String field, double start, double end, double bucketSize) {
        return new Group(GroupType.HISTOGRAM_GROUP)
                .setHistogramGroup(new HistogramGroup()
                        .setField(field)
                        .setBucketSize(bucketSize)
                        .setStartPoint(start)
                        .setEndPoint(end)
                );
    }
}
