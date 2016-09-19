/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Visitor;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.sql.RelationalPath;

import javax.annotation.Nullable;
import java.util.List;

/**
 * In complex queries with subqueries we need to use expressions from GROUP BY in ON clause with joined subqueries.
 * If GROUP BY expression is not just a field name (CASE clause, function etc.), QueryDSL will not use it properly in
 * ON. Instead, we need to use original (non-aliased) expression or call it by alias.
 * <p>
 * This class allows to use both original and aliased expression in different parts of queries.
 * <p>
 * For example, in SELECT we need to write CASE ... END AS "alias", in ON - just alias.
 * <p>
 * If you want to be imbued with this, just execute query that have both LAST_VALUE or PERCENTILE metric and
 * HISTOGRAM_GROUP or TIME_GROUP grouping.

 * @see Utils#createFieldsEqualPredicate(List, List, RelationalPath)
 */
public class AliasedComparableExpressionBase<T extends Comparable> extends ComparableExpressionBase<T> {
    private String alias;
    private ComparableExpressionBase<T> original;

    public static <T extends Comparable> AliasedComparableExpressionBase<T> create(
            ComparableExpressionBase<T> expression, String alias) {
        AliasedComparableExpressionBase<T> obj;
        if (alias == null) {
            obj = new AliasedComparableExpressionBase<>(expression);
            obj.original = expression;
        } else {
            obj = new AliasedComparableExpressionBase<>(expression.as(alias));
            obj.alias = alias;
            obj.original = expression;
        }
        return obj;
    }

    public AliasedComparableExpressionBase<T> withoutAlias() {
        return create(original, null);
    }

    private AliasedComparableExpressionBase(Expression<T> mixin) {
        super(mixin);
    }

    @Nullable
    @Override
    public <R, C> R accept(Visitor<R, C> v, C context) {
        return mixin.accept(v, context);
    }

    public boolean hasAlias() {
        return alias != null;
    }

    public String getAliasName() {
        return alias;
    }

    public ComparableExpressionBase<T> original() {
        return original;
    }
}
