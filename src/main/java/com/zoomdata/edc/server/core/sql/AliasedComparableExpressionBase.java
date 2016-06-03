/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Visitor;
import com.querydsl.core.types.dsl.ComparableExpressionBase;

import javax.annotation.Nullable;

public class AliasedComparableExpressionBase<T extends Comparable> extends ComparableExpressionBase<T> {
    private String alias;
    private ComparableExpressionBase<T> original;

    public static <T extends Comparable> AliasedComparableExpressionBase<T> create(
            ComparableExpressionBase<T> expression, String alias) {
        AliasedComparableExpressionBase<T> obj;
        if (alias == null) {
            obj = new AliasedComparableExpressionBase<T>(expression);
            obj.original = expression;
        } else {
            obj = new AliasedComparableExpressionBase<T>(expression.as(alias));
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
