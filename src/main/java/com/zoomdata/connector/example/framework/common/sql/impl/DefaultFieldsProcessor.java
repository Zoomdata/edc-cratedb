/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.Wildcard;
import com.zoomdata.connector.example.framework.common.sql.FieldsProcessor;

import java.util.List;
import java.util.stream.Collectors;

import static com.querydsl.core.types.dsl.Expressions.stringPath;


public class DefaultFieldsProcessor implements FieldsProcessor {
    protected Path<?> table;
    protected List<String> thriftFields;
    protected List<Expression> select;

    @Override
    public List<Expression> process(Path<?> table, List<String> fields) {
        this.table = table;
        this.thriftFields = fields;

        select = fields.stream()
                .map(f -> "*".equals(f) ? Wildcard.all : createPath(table, f))
                .collect(Collectors.toList());

        return select;
    }

    protected Expression createPath(Path<?> table, String field) {
        return stringPath(table, field);
    }

    @Override
    public Path<?> getTable() {
        return table;
    }

    @Override
    public List<String> getThriftFields() {
        return thriftFields;
    }

    @Override
    public List<Expression> getSelect() {
        return select;
    }
}
