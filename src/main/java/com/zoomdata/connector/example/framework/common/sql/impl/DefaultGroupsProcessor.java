/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.Operator;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.*;
import com.zoomdata.connector.example.framework.common.sql.GroupsProcessor;
import com.zoomdata.connector.example.framework.common.sql.ops.ExtendedDateTimeOps;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.group.GroupType;
import com.zoomdata.gen.edc.group.HistogramGroup;
import com.zoomdata.gen.edc.group.TimeGroup;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldType;
import com.zoomdata.gen.edc.types.TimeGranularity;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.*;

import static com.google.common.collect.Iterables.getLast;
import static com.querydsl.core.types.dsl.Expressions.*;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.SECONDS_FORMAT;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.YEAR_FORMAT;


public class DefaultGroupsProcessor implements GroupsProcessor {

    public static final String HISTOGRAM_MSG_START_GREATER_END = "Start point of histogram group is greater then end point.";
    public static final String HISTOGRAM_MSG_NEGATIVE_BUCKET_SIZE = "Bucket size is less or equal to zero.";
    public static final String HISTOGRAM_MSG_LESS_THAN_ONE_BUCKET = "Histogram should have more then one bucket!";

    protected Path<?> table;
    protected List<Group> thriftGroups;
    protected Optional<String> serverTimeZone = Optional.empty();

    protected List<AliasedComparableExpressionBase> groupBy;

    protected Map<Group, ComparableExpressionBase> groupExpressions = new HashMap<>();

    public static final String HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX = "__hg_";
    public static final String TIME_GROUP_FIELD_ALIAS_PREFIX = "__tg_";

    public DefaultGroupsProcessor() {
    }

    public DefaultGroupsProcessor(Optional<String> serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
    }

    @Override
    public List<AliasedComparableExpressionBase> process(Path<?> table, List<Group> groups, Map<String, FieldMetadata> fieldMetadata) {
        this.table = table;
        this.thriftGroups = groups;

        groupBy = new ArrayList<>(groups.size());

        AliasedComparableExpressionBase e;
        for (Group g : groups) {
            GroupType type = g.getType();
            switch (type) {
                case ATTRIBUTE_GROUP: {
                    e = processATTRIBUTE_GROUP(table, g, fieldMetadata);
                    break;
                }
                case HISTOGRAM_GROUP: {
                    e = processHISTOGRAM_GROUP(g);
                    break;
                }
                case TIME_GROUP: {
                    e = processTIME_GROUP(g, fieldMetadata.get(g.getTimeGroup().getField()));
                    break;
                }
                default: {
                    throw new IllegalStateException("Group of type " + type + " is not supported.");
                }
            }
            groupBy.add(e);
            groupExpressions.put(g, e);
        }

        return groupBy;
    }

    @Override
    public Path<?> getTable() {
        return table;
    }

    @Override
    public List<Group> getThriftGroups() {
        return thriftGroups;
    }

    public List<AliasedComparableExpressionBase> getGroupBy() {
        return groupBy;
    }

    @Override
    public ComparableExpressionBase getGroupExpression(Group group) {
        return groupExpressions.get(group);
    }

    // ==================== GROUPS IMPLEMENTATION ====================

    protected AliasedComparableExpressionBase processATTRIBUTE_GROUP(Path<?> table, Group g, Map<String, FieldMetadata> fieldMetadata) {
        String field = g.getAttributeGroup().getField();
        FieldMetadata metadata = fieldMetadata.get(field);
        ComparableExpressionBase<?> path;
        switch (metadata.getType()) {
            case DATE:
                path = dateTimePath(Date.class, table, field);
                break;
            case INTEGER:
                path = numberPath(Integer.class, table, field);
                break;
            case DOUBLE:
                path = numberPath(Double.class, table, field);
                break;
            case STRING:
                path = stringPath(table, field);
                break;
            default:
                throw new IllegalArgumentException("Type mapping not found for type: " + metadata.getType());
        }
        return AliasedComparableExpressionBase.create(path, null);
    }

    protected AliasedComparableExpressionBase processHISTOGRAM_GROUP(Group g) {
        HistogramGroup group = g.getHistogramGroup();
        double[] points = getHistogramGroupPoints(group);
        NumberPath<Double> field = numberPath(Double.class, table, group.getField());

        // Starting point: ( -inf; start ]
        CaseBuilder.Cases<String, StringExpression> cases = cases()
                .when(field.loe(doubleTemplate(points[0]))).then(";" + points[0]);
        // Several points. Every sub-interval is (x, y]
        for (int p = 1; p < points.length; p++) {
            cases.when(field.gt(doubleTemplate(points[p - 1])).and(field.loe(doubleTemplate(points[p]))))
                    .then(points[p - 1] + ";" + points[p]);
        }
        // Last point: (end; +inf)
        StringExpression expression = cases.otherwise(points[points.length - 1] + ";");

        return AliasedComparableExpressionBase.create(expression,
                getHistogramGroupFieldAliasPrefix() + Utils.underscore(group.getField()));
    }

    double[] getHistogramGroupPoints(HistogramGroup group) {
        BigDecimal startPoint = BigDecimal.valueOf(group.getStartPoint()).stripTrailingZeros();
        BigDecimal endPoint = BigDecimal.valueOf(group.getEndPoint()).stripTrailingZeros();
        BigDecimal bucketSize = BigDecimal.valueOf(group.getBucketSize()).stripTrailingZeros();

        if (startPoint.compareTo(endPoint) >= 0) {
            throw new IllegalArgumentException(HISTOGRAM_MSG_START_GREATER_END + printHistogram(group));
        }
        if (bucketSize.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(HISTOGRAM_MSG_NEGATIVE_BUCKET_SIZE + printHistogram(group));
        }
        if (endPoint.subtract(startPoint).compareTo(bucketSize) <= 0) {
            throw new IllegalArgumentException(HISTOGRAM_MSG_LESS_THAN_ONE_BUCKET + printHistogram(group));
        }

        List<BigDecimal> points = new ArrayList<>();
        points.add(startPoint.add(bucketSize));
        while (getLast(points).add(bucketSize).compareTo(endPoint) < 0) {
            points.add(getLast(points).add(bucketSize));
        }
        return points.stream().mapToDouble(BigDecimal::doubleValue).toArray();
    }

    private String printHistogram(HistogramGroup histogram) {
        return String.format(" Start: %f, end: %f, bucketSize: %f", histogram.getStartPoint(), histogram.getEndPoint(),
                histogram.getBucketSize());
    }

    protected AliasedComparableExpressionBase processTIME_GROUP(Group g, FieldMetadata fieldMetadata) {
        TimeGroup group = g.getTimeGroup();
        String field = group.getField();
        TimeGranularity granularity = group.getGranularity();

        if (StringUtils.equals(fieldMetadata.getFieldParams().getTimeFieldPattern(), YEAR_FORMAT)) {
            checkYearPatternGranularity(fieldMetadata);
            return AliasedComparableExpressionBase.create(
                dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_YEARPATTERN, numberPath(Long.class, table, field)),
                getTimeGroupFieldAliasPrefix() + granularity.name().toLowerCase() + "_over_" +
                    Utils.underscore(field));
        }
        Operator op;
        switch (granularity) {
            case MILLISECOND: {
                throw new UnsupportedOperationException(granularity + " is not supported");
            }
            case SECOND: {
                op = Ops.DateTimeOps.TRUNC_SECOND;
                break;
            }
            case MINUTE: {
                op = Ops.DateTimeOps.TRUNC_MINUTE;
                break;
            }
            case HOUR: {
                op = Ops.DateTimeOps.TRUNC_HOUR;
                break;
            }
            case DAY: {
                op = Ops.DateTimeOps.TRUNC_DAY;
                break;
            }
            case WEEK: {
                op = Ops.DateTimeOps.TRUNC_WEEK;
                break;
            }
            case MONTH: {
                op = Ops.DateTimeOps.TRUNC_MONTH;
                break;
            }
            case QUARTER: {
                op = ExtendedDateTimeOps.TRUNC_QUARTER;
                break;
            }
            case YEAR: {
                op = Ops.DateTimeOps.TRUNC_YEAR;
                break;
            }
            default: {
                throw new IllegalStateException("Time group with granularity " + granularity + " is not supported.");
            }
        }


        final DateTimeExpression<Date> dateDateTimePath;
        if (fieldMetadata.getType() == FieldType.DATE) {
            dateDateTimePath = dateTimePath(Date.class, table, field);
        } else if (StringUtils.equals(fieldMetadata.getFieldParams().getTimeFieldPattern(), SECONDS_FORMAT)) {
            dateDateTimePath = dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_UNIXTIME, numberPath(Long.class, table, field));
        } else {
            checkTimeGroupFieldType(fieldMetadata);
            dateDateTimePath = dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_MILLIS, numberPath(Long.class, table, field));
        }

        DateOperation<Date> dateFunc = dateOperation(Date.class, op,dateDateTimePath);
        return AliasedComparableExpressionBase.create(dateFunc,
                getTimeGroupFieldAliasPrefix() + granularity.name().toLowerCase() + "_over_" + Utils.underscore(field));
    }

    private void checkTimeGroupFieldType(FieldMetadata fieldMetadata) {
        if (fieldMetadata.getType() == FieldType.STRING) {
            throw new IllegalArgumentException("String pattern do not support " + fieldMetadata);
        }
    }

    private void checkYearPatternGranularity(FieldMetadata fieldMetadata) {
        if (fieldMetadata.getFieldParams().getTimeFieldGranularity() != TimeGranularity.YEAR) {
            throw new RuntimeException("Year column can't have granularity " +
                fieldMetadata.getFieldParams().getTimeFieldGranularity());
        }
    }

    public static NumberTemplate<Double> doubleTemplate(double val) {
        return numberTemplate(Double.class, String.valueOf(val));
    }

    protected String getTimeGroupFieldAliasPrefix() {
        return TIME_GROUP_FIELD_ALIAS_PREFIX;
    }

    protected String getHistogramGroupFieldAliasPrefix() {
        return HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX;
    }
}
