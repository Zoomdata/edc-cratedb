/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.zoomdata.gen.edc.request.StatField;
import com.zoomdata.gen.edc.request.StatFunction;

import java.util.ArrayList;
import java.util.List;

import static com.querydsl.core.types.dsl.Expressions.numberPath;

public class StatsProcessor {
    protected Path<?> table;
    protected List<StatField> thriftStats;
    protected List<ComparableExpressionBase> select;

    public List<ComparableExpressionBase> process(Path<?> table, List<StatField> stats) {
        this.table = table;
        this.thriftStats = stats;

        select = new ArrayList<>(stats.size());
        for (StatField s : stats) {
            StatFunction type = s.getStat();
            ComparableExpressionBase e;
            switch (type) {
                case MIN: {
                    e = numberPath(Double.class, table, s.getField()).min();
                    break;
                }
                case MAX: {
                    e = numberPath(Double.class, table, s.getField()).max();
                    break;
                }
                default: {
                    throw new IllegalStateException("Metric of type " + type + " is not supported.");
                }
            }
            select.add(e);
        }
        return select;
    }

    public Path<?> getTable() {
        return table;
    }

    public List<StatField> getThriftStats() {
        return thriftStats;
    }

    public List<ComparableExpressionBase> getSelect() {
        return select;
    }
}
