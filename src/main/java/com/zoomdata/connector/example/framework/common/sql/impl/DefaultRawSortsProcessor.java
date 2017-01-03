/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.StringPath;
import com.zoomdata.connector.example.framework.common.sql.RawSortsProcessor;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.sort.SortDir;

import java.util.ArrayList;
import java.util.List;

import static com.querydsl.core.types.dsl.Expressions.stringPath;


public class DefaultRawSortsProcessor implements RawSortsProcessor {
    protected Path<?> table;
    protected List<RawSort> thriftSorts;
    protected List<OrderSpecifier> orderBy;

    @Override
    public List<OrderSpecifier> process(Path<?> table, List<RawSort> sorts) {
        this.table = table;
        this.thriftSorts = sorts;

        orderBy = new ArrayList<>(sorts.size());
        for (RawSort s : sorts) {
            StringPath sortField = stringPath(table, s.getField());
            final OrderSpecifier<String> orderSpecifier;
            if (SortDir.DESC == s.getDirection()) {
                orderSpecifier = sortField.desc();
            } else {
                orderSpecifier = sortField.asc();
            }
            orderBy.add(orderSpecifier);
        }
        return orderBy;
    }

    @Override
    public Path<?> getTable() {
        return table;
    }

    @Override
    public List<RawSort> getThriftSorts() {
        return thriftSorts;
    }

    @Override
    public List<OrderSpecifier> getOrderBy() {
        return orderBy;
    }
}
