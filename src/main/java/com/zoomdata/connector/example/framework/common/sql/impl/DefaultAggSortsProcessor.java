/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.zoomdata.connector.example.framework.common.sql.AggSortsProcessor;
import com.zoomdata.connector.example.framework.common.sql.IGroupExpressionProducer;
import com.zoomdata.connector.example.framework.common.sql.MetricsProcessor;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.metric.Metric;
import com.zoomdata.gen.edc.sort.AggSort;
import com.zoomdata.gen.edc.sort.SortDir;
import com.zoomdata.gen.edc.sort.SortType;

import java.util.ArrayList;
import java.util.List;

public class DefaultAggSortsProcessor implements AggSortsProcessor {
    protected Path<?> table;
    protected List<AggSort> thriftAggSorts;
    protected List<OrderSpecifier> orderBy;

    @Override
    public List<OrderSpecifier> process(Path<?> table, List<AggSort> sorts,
                                        MetricsProcessor metricsProcessor, IGroupExpressionProducer groupsProcessor) {
        this.table = table;
        this.thriftAggSorts = sorts;

        orderBy = new ArrayList<>(sorts.size());

        for (AggSort s : sorts) {
            SortType type = s.getType();
            ComparableExpressionBase sortField;
            switch (type) {
                case GROUP: {
                    Group g = s.getGroup();
                    sortField = groupsProcessor.getExpressionForOrderBy(g);
                    if (sortField == null) {
                        throw new IllegalArgumentException("Group to sort on is unknown. Make sure that you " +
                                "called withGroups(List<Group>) method with the group as parameter before this one.");
                    }
                    break;
                }
                case METRIC: {
                    Metric m = s.getMetric();
                    sortField = metricsProcessor.getMetricExpression(m);
                    if (sortField == null) {
                        throw new IllegalArgumentException("Metric to sort on is unknown. Make sure that you " +
                                "called withMetrics(List<Metric>) method with the metric as parameter before this one.");
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException("AggSort of type " + type + " is not supported.");
                }
            }

            if (SortDir.DESC == s.getDirection()) {
                orderBy.add(sortField.desc());
            } else {
                orderBy.add(sortField.asc());
            }
        }
        return orderBy;
    }
}
