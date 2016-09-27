/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.*;
import com.zoomdata.connector.example.common.utils.ThriftUtils;
import com.zoomdata.connector.example.framework.common.op.BinaryOp;
import com.zoomdata.connector.example.framework.common.op.UnaryOp;
import com.zoomdata.gen.edc.metric.MetricPercentile;
import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldType;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static com.querydsl.core.types.dsl.Expressions.*;

public final class Utils {

    public static final String CUSTOM_SQL_PARAM_KEY = "CUSTOM_SQL";
    public static final String YEAR_FORMAT = "yyyy";
    public static final String SECONDS_FORMAT = "sssssssss";
    public static final String DEFAULT_SERVER_TIME_ZONE = "UTC";

    private Utils() {
    }

    @SuppressWarnings("unchecked")
    public static BooleanBuilder createFieldsEqualPredicate(List<AliasedComparableExpressionBase> leftExpressions,
                                                            List<AliasedComparableExpressionBase> rightExpressions,
                                                            Path<?> rightTable) {
        if (leftExpressions.size() != rightExpressions.size()) {
            throw new IllegalArgumentException("Length of expressions array must be equal.");
        }

        BooleanBuilder predicate = new BooleanBuilder();

        Iterator<AliasedComparableExpressionBase> leftIt = leftExpressions.iterator();
        Iterator<AliasedComparableExpressionBase> rightIt = rightExpressions.iterator();
        while (leftIt.hasNext()) {
            AliasedComparableExpressionBase leftExpression = leftIt.next();
            AliasedComparableExpressionBase rightExpression = rightIt.next();

            if (rightExpression.getAliasName() == null) {
                predicate.and(leftExpression.original().eq(rightExpression.original()));
            } else {
                predicate.and(leftExpression.original().eq(
                        simplePath(rightExpression.getType(), rightTable, rightExpression.getAliasName())));
            }
        }
        return predicate;
    }

    @SuppressWarnings("unchecked")
    public static BooleanBuilder createFieldsEqualCoalescePredicate(List<AliasedComparableExpressionBase> leftExpressions,
                                                                    List<AliasedComparableExpressionBase> rightExpressions,
                                                                    Path<?> rightTable) {
        if (leftExpressions.size() != rightExpressions.size()) {
            throw new IllegalArgumentException("Length of expressions array must be equal.");
        }

        BooleanBuilder predicate = new BooleanBuilder();

        Iterator<AliasedComparableExpressionBase> leftIt = leftExpressions.iterator();
        Iterator<AliasedComparableExpressionBase> rightIt = rightExpressions.iterator();
        while (leftIt.hasNext()) {
            AliasedComparableExpressionBase leftExpression = leftIt.next();
            AliasedComparableExpressionBase rightExpression = rightIt.next();

            Coalesce leftCoalesce = leftExpression.original().coalesce(coalescePlaceholderFor(leftExpression.getType()));
            Coalesce rightCoalesce;
            if (rightExpression.getAliasName() == null) {
                rightCoalesce = rightExpression.original().coalesce(coalescePlaceholderFor(rightExpression.getType()));
            } else {
                SimplePath path = simplePath(rightExpression.getType(), rightTable, rightExpression.getAliasName());
                rightCoalesce = new Coalesce<>(rightExpression.getType(), path)
                        .add(coalescePlaceholderFor(rightExpression.getType()));
            }
            predicate.and(leftCoalesce.asString().eq(rightCoalesce.asString()));
        }
        return predicate;
    }

    public static <T> Expression<T> coalescePlaceholderFor(Class<T> type) {
        if (Number.class.isAssignableFrom(type)) {
            return Expressions.constant((T) new Integer(987654321));
        } else if (Date.class.isAssignableFrom(type)) {
            return Expressions.constant((T) new Date(9876543210123L));
        }
        return Expressions.constant((T) "COALESCE_987654321_PLACEHOLDER");
    }

    public static Expression[] combine(List<? extends Expression> expressions,
                                       Expression... otherExpressions) {
        Expression[] result = new Expression[expressions.size() + otherExpressions.length];
        expressions.toArray(result);
        System.arraycopy(otherExpressions, 0, result, expressions.size(), otherExpressions.length);
        return result;
    }

    /**
     * Unites several lists to one array.
     * @param expressionLists lists of expressions. Allows <code>null</code> arguments that will be skipped.
     * @return array of expressions containing in all lists according to
     * order of lists and order of expressions inside lists.
     */
    @SafeVarargs
    public static Expression[] combineLists(List<? extends Expression>... expressionLists) {
        int length = Arrays.stream(expressionLists).filter(l -> l != null).mapToInt(List::size).sum();

        Expression[] result = new Expression[length];

        int i = 0;
        for (List<? extends Expression> list : expressionLists) {
            if (list == null) {
                continue;
            }
            for (Expression e : list) {
                result[i++] = e;
            }
        }
        return result;
    }

    public static String underscore(String field) {
        return field.replace(' ', '_').replace('.', '_');
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] toArray(Class<T> clazz, List<? extends T> list) {
        T[] array = (T[]) Array.newInstance(clazz, list.size());
        return list.toArray(array);
    }

    public static Predicate createPredicateUnary(
        Path<?> table,
            FieldType type, String filterPath,
            UnaryOp<NumberPath, BooleanExpression> numberProcessor,
            UnaryOp<StringPath, BooleanExpression> stringProcessor,
            UnaryOp<DateTimePath, BooleanExpression> dateProcessor) {
        switch (type) {
            case INTEGER: {
                return numberProcessor.apply(numberPath(Long.class, table, filterPath));
            }
            case DOUBLE: {
                return numberProcessor.apply(numberPath(Double.class, table, filterPath));
            }
            case STRING: {
                return stringProcessor.apply(stringPath(table, filterPath));
            }
            case DATE: {
                return dateProcessor.apply(dateTimePath(Date.class, table, filterPath));
            }
            default: {
                throw new IllegalStateException("Unknown FieldType " + type);
            }
        }
    }

    public static Predicate createPredicateBinary(
        Path<?> table,
            FieldType type, Field value, String filterPath,
            BinaryOp<NumberPath, Expression, BooleanExpression> numberProcessor,
            BinaryOp<StringPath, Expression, BooleanExpression> stringProcessor,
            BinaryOp<DateTimePath, Expression, BooleanExpression> dateProcessor) {
        if (value.isIsNull()) {
            throw new IllegalArgumentException("Field has NULL value in binary predicate.");
        }

        switch (type) {
            case INTEGER: {
                NumberPath leftOperand = numberPath(Long.class, table, filterPath);
                Expression rightOperand = constant(ThriftUtils.getInteger(value));
                return numberProcessor.apply(leftOperand, rightOperand);
            }
            case DOUBLE: {
                NumberPath leftOperand = numberPath(Double.class, table, filterPath);
                Expression rightOperand = constant(ThriftUtils.getDouble(value));
                return numberProcessor.apply(leftOperand, rightOperand);
            }
            case STRING: {
                StringPath leftOperand = stringPath(table, filterPath);
                Expression rightOperand = constant(ThriftUtils.getString(value));
                return stringProcessor.apply(leftOperand, rightOperand);
            }
            case DATE: {
                DateTimePath leftOperand = dateTimePath(Date.class, table, filterPath);
                @SuppressWarnings("ConstantConditions")
                Expression rightOperand = constant(ThriftUtils.getDateTime(value));
                return dateProcessor.apply(leftOperand, rightOperand);
            }
            default: {
                throw new IllegalStateException("Unknown FieldType " + type);
            }
        }
    }

    public static Predicate createPredicateVarary(
        Path<?> table,
            FieldType type, String filterPath, List<Field> values,
            BinaryOp<SimpleExpression, List<Object>, BooleanExpression> simpleProcessor) {
        switch (type) {
            case INTEGER: {
                NumberPath leftOperand = numberPath(Long.class, table, filterPath);
                List<Object> rightOperand = values.stream().map(ThriftUtils::getInteger).collect(Collectors.toList());
                return simpleProcessor.apply(leftOperand, rightOperand);
            }
            case DOUBLE: {
                NumberPath leftOperand = numberPath(Double.class, table, filterPath);
                List<Object> rightOperand = values.stream().map(ThriftUtils::getDouble).collect(Collectors.toList());
                return simpleProcessor.apply(leftOperand, rightOperand);
            }
            case STRING: {
                StringPath leftOperand = stringPath(table, filterPath);
                List<Object> rightOperand = values.stream().map(ThriftUtils::getString).collect(Collectors.toList());
                return simpleProcessor.apply(leftOperand, rightOperand);
            }
            case DATE: {
                DateTimePath leftOperand = dateTimePath(Date.class, table, filterPath);
                List<Object> rightOperand = values.stream().map(ThriftUtils::getDateTime).collect(Collectors.toList());
                return simpleProcessor.apply(leftOperand, rightOperand);
            }
            default: {
                throw new IllegalStateException("Unknown FieldType " + type);
            }
        }
    }

    public static String createPercentileRowNumberAliasName(MetricPercentile percentile) {
        return createPercentileRowNumberAliasName(String.valueOf(percentile.getMargin()));
    }

    public static String createPercentileRowNumberAliasName(String percentile) {
        return "prcnt_" + percentile + "_row_number";
    }

    public static Boolean isCustomSql(CollectionInfo collectionInfo) {
        final Map<String, String> collectionParams = collectionInfo.getParams();
        if (collectionParams != null) {
            return Boolean.valueOf(collectionParams.getOrDefault(CUSTOM_SQL_PARAM_KEY, "false"));
        }
        return false;
    }

}
