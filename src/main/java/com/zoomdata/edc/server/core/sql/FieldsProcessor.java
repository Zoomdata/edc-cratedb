/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.Wildcard;

import java.util.List;
import java.util.stream.Collectors;

import static com.querydsl.core.types.dsl.Expressions.stringPath;

public class FieldsProcessor {
    protected Path<?> table;
    protected List<String> thriftFields;
    protected List<Expression> select;

    public List<Expression> process(Path<?> table, List<String> fields) {
        this.table = table;
        this.thriftFields = fields;

        select = fields.stream()
                .map(f -> "*".equals(f) ? Wildcard.all : createPath(table, f))
                .collect(Collectors.toList());

        return select;
    }

    protected StringPath createPath(Path<?> table, String field) {
        return stringPath(table, field);
    }

    public Path<?> getTable() {
        return table;
    }

    public List<String> getThriftFields() {
        return thriftFields;
    }

    public List<Expression> getSelect() {
        return select;
    }
}
