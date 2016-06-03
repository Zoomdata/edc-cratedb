/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.StringPath;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.sort.SortDir;

import java.util.ArrayList;
import java.util.List;

import static com.querydsl.core.types.dsl.Expressions.stringPath;

public class RawSortsProcessor{
    protected Path<?> table;
    protected List<RawSort> thriftSorts;
    protected List<OrderSpecifier> orderBy;

    public List<OrderSpecifier> process(Path<?> table, List<RawSort> sorts) {
        this.table = table;
        this.thriftSorts = sorts;

        orderBy = new ArrayList<>(sorts.size());
        for (RawSort s : sorts) {
            StringPath sortField = stringPath(table, s.getField());
            if (SortDir.DESC == s.getDirection()) {
                orderBy.add(sortField.desc());
            } else {
                orderBy.add(sortField.asc());
            }
        }
        return orderBy;
    }

    public Path<?> getTable() {
        return table;
    }

    public List<RawSort> getThriftSorts() {
        return thriftSorts;
    }

    public List<OrderSpecifier> getOrderBy() {
        return orderBy;
    }
}
