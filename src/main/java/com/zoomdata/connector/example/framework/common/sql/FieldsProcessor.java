/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;

import java.util.List;

public interface FieldsProcessor {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param fields fields.
     * @return list of expressions for SELECT.
     */
    List<Expression> process(Path<?> table, List<String> fields);

    /**
     * Returns table.
     * @return table or <code>null</code> if processor hasn't been initialized.
     */
    Path<?> getTable();

    /**
     * Returns list of processed fields.
     * @return list of field or <code>null</code> if processor hasn't been initialized.
     */
    List<String> getThriftFields();

    /**
     * Returns result of {@link #process(Path, List)} method execution.
     * @return list of SELECT expressions or <code>null</code> if processor hasn't been initialized.
     */
    List<Expression> getSelect();
}
