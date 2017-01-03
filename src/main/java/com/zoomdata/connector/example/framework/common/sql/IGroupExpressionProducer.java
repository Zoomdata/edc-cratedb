/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql;

import com.querydsl.core.types.Path;
import com.zoomdata.connector.example.framework.common.sql.impl.AliasedComparableExpressionBase;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.util.List;
import java.util.Map;

public interface IGroupExpressionProducer {
    /**
     * Initializes processor and does all work.
     * @param table table.
     * @param groups groups.
     * @param fieldMetadata
     * @return initialized processor.
     */

    // TODO construction logic details should be separated from the interface
    IGroupExpressionProducer process(
            Path<?> table,
            List<Group> groups,
            Map<String, FieldMetadata> fieldMetadata
    );

    /**
     * Returns list of processed groups.
     * @return list of groups or <code>null</code> if processor hasn't been initialized.
     */
    List<Group> getThriftGroups();

    /**
     * Returns result of {@link #process(Path, List, Map)} method execution, which is modified
     * for select clause
     * @return list of expressions or <code>null</code> if processor hasn't been initialized.
     */
    List<AliasedComparableExpressionBase> getExpressionsForSelect();

    /**
     * Returns result of {@link #process(Path, List, Map)} method execution, which is modified
     * for group by clause
     * @return list of expressions or <code>null</code> if processor hasn't been initialized.
     */
    List<AliasedComparableExpressionBase> getExpressionsForGroupBy();

    /**
     * Returns GROUP BY expression by processed group. Intended for order by clause.
     * @return expression or <code>null</code> if processor hasn't been initialized or
     * there is no expression for this group.
     */
    AliasedComparableExpressionBase getExpressionForOrderBy(Group group);
}
