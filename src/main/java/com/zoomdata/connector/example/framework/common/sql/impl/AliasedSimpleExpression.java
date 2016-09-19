/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Visitor;
import com.querydsl.core.types.dsl.SimpleExpression;

import javax.annotation.Nullable;

import static com.querydsl.core.types.dsl.Expressions.simplePath;

public class AliasedSimpleExpression<T> extends SimpleExpression<T> {

    private String alias;
    private SimpleExpression<T> original;

    public static <T> AliasedSimpleExpression<T> create(SimpleExpression<T> expression, String alias) {
        AliasedSimpleExpression<T> obj;
        if (alias == null) {
            obj = new AliasedSimpleExpression<>(expression);
            obj.original = expression;
        } else {
            obj = new AliasedSimpleExpression<>(expression.as(alias));
            obj.alias = alias;
            obj.original = expression;
        }
        return obj;
    }

    public AliasedSimpleExpression<T> withoutAlias() {
        return create(original, null);
    }

    private AliasedSimpleExpression(Expression<T> mixin) {
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

    public SimpleExpression<T> original() {
        return original;
    }

    public Path<?> getPath() {
        return alias == null ? null : simplePath(Object.class, alias);
    }

    public AliasedSimpleExpression<T> withNewAlias(String newAlias) {
        return create(original, newAlias);
    }
}
