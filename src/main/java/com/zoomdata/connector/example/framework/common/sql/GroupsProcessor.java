/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.zoomdata.connector.example.framework.common.sql.impl.AliasedComparableExpressionBase;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.List;
import java.util.Map;


public interface GroupsProcessor {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param groups groups.
     * @param fieldMetadata
     * @return list of expressions for GROUP BY.
     */
    List<AliasedComparableExpressionBase> process(Path<?> table, List<Group> groups, Map<String, FieldMetadata> fieldMetadata);

    /**
     * Returns table.
     * @return table or <code>null</code> if processor hasn't been initialized.
     */
    Path<?> getTable();

    /**
     * Returns list of processed groups.
     * @return list of groups or <code>null</code> if processor hasn't been initialized.
     */
    List<Group> getThriftGroups();

    /**
     * Returns result of {@link #process(Path, List, Map)} method execution.
     * @return list of expressions or <code>null</code> if processor hasn't been initialized.
     */
    List<AliasedComparableExpressionBase> getGroupBy();

    /**
     * Returns GROUP BY expression by processed group.
     * @return expression or <code>null</code> if processor hasn't been initialized or
     * there is no expression for this group.
     */
    ComparableExpressionBase getGroupExpression(Group group);
}
