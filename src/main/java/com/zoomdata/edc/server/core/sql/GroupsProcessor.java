/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.types.Operator;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.DateOperation;
import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.NumberTemplate;
import com.querydsl.core.types.dsl.StringExpression;
import com.zoomdata.edc.server.core.sql.*;
import com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase;
import com.zoomdata.edc.server.core.utils.SQLUtils;
import com.zoomdata.edc.server.core.utils.StringUtils;
import com.zoomdata.gen.edc.group.AttributeGroup;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.group.GroupType;
import com.zoomdata.gen.edc.group.HistogramGroup;
import com.zoomdata.gen.edc.group.TimeGroup;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldType;
import com.zoomdata.gen.edc.types.TimeGranularity;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.querydsl.core.types.dsl.Expressions.*;
import static com.querydsl.core.types.dsl.Expressions.dateTimeOperation;

public class GroupsProcessor {
    protected Path<?> table;
    protected List<Group> thriftGroups;

    protected List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> groupBy;

    protected Map<Group, ComparableExpressionBase> groupExpressions = new HashMap<>();

    public static final String HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX = "__hg_";
    public static final String TIME_GROUP_FIELD_ALIAS_PREFIX = "__tg_";

    public List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> process(Path<?> table, List<Group> groups, Map<String, FieldMetadata> fieldMetadata) {
        this.table = table;
        this.thriftGroups = groups;

        groupBy = new ArrayList<>(groups.size());

        com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase e;
        for (Group g : groups) {
            GroupType type = g.getType();
            switch (type) {
                case ATTRIBUTE_GROUP: {
                    e = processATTRIBUTE_GROUP(table, g);
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

    public Path<?> getTable() {
        return table;
    }

    public List<Group> getThriftGroups() {
        return thriftGroups;
    }

    public List<com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase> getGroupBy() {
        return groupBy;
    }

    public ComparableExpressionBase getGroupExpression(Group group) {
        return groupExpressions.get(group);
    }

    // ==================== GROUPS IMPLEMENTATION ====================

    protected com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase processATTRIBUTE_GROUP(Path<?> table, Group g) {
        AttributeGroup group = g.getAttributeGroup();
        return com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase.create(stringPath(table, group.getField()), null);
    }

    protected com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase processHISTOGRAM_GROUP(Group g) {
        HistogramGroup group = g.getHistogramGroup();
        String field = group.getField();
        double[] points = getHistogramGroupPoints(group);

        NumberPath<Double> f = numberPath(Double.class, table, field);

        StringExpression expression;

        if (points.length == 1) {
            // Only point, start = end: bucket is [x; x], in other words include only values equal to point.
            expression = Expressions.stringTemplate(group.getStartPoint()+ ";"+ group.getStartPoint());
        } else {
            // Before start point: ( -inf; start )
            CaseBuilder.Cases<String, StringExpression> cases = cases()
                .when(f.loe(doubleTemplate(points[0]))).then(";" + points[0]);
            // Several points. Every sub-interval is [x, y), except last one that is [x, y]
            for (int p = 1; p < points.length - 1; p++) {
                cases.when(f.gt(doubleTemplate(points[p - 1])).and(f.loe(doubleTemplate(points[p]))))
                        .then(points[p - 1] + ";" + points[p]);
            }
            // After end point: (end; +inf)
            expression = cases.otherwise(points[points.length - 1] + ";");
        }

        return com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase.create(expression,
                getHistogramGroupFieldAliasPrefix() + SQLUtils.underscore(field));
    }

    protected com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase processTIME_GROUP(Group g, FieldMetadata fieldMetadata) {
        TimeGroup group = g.getTimeGroup();
        String field = group.getField();
        TimeGranularity granularity = group.getGranularity();

        if (StringUtils.equals(fieldMetadata.getFieldParams().getTimeFieldPattern(), StringUtils.YEAR_FORMAT)) {
            checkYearPatternGranularity(fieldMetadata);
            return com.zoomdata.edc.server.core.sql.AliasedComparableExpressionBase.create(
                    dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_YEARPATTERN, numberPath(Long.class, table, field)),
                    getTimeGroupFieldAliasPrefix() + granularity.name().toLowerCase() + "_over_" +
                            SQLUtils.underscore(field));
        }
        Operator op;
        switch (granularity) {
            case MILLISECOND: {
                op = ExtendedDateTimeOps.TRUNC_MILLISECOND;
                break;
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
        } else if (StringUtils.equals(fieldMetadata.getFieldParams().getTimeFieldPattern(), StringUtils.SECONDS_FORMAT)) {
            dateDateTimePath = dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_UNIXTIME, numberPath(Long.class, table, field));
        } else {
            checkTimeGroupFieldType(fieldMetadata);
            dateDateTimePath = dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_MILLIS, numberPath(Long.class, table, field));
        }

        DateOperation<Date> dateFunc = dateOperation(Date.class, op, dateDateTimePath);

        return AliasedComparableExpressionBase.create(dateFunc,
                getTimeGroupFieldAliasPrefix() + granularity.name().toLowerCase() + "_over_" +
                        SQLUtils.underscore(field));
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

    public static double[] getHistogramGroupPoints(HistogramGroup group) {
        double startPoint = group.getStartPoint();
        double endPoint = group.getEndPoint();

        if (startPoint > endPoint) {
            throw new IllegalStateException("Start point of histogram group is greater then end point.");
        }

        if (startPoint == endPoint) {
            return new double[]{startPoint};
        }

        double intervalLength = endPoint - startPoint;

        double bucketSize;
        int bucketCount;

        bucketSize = group.getBucketSize();
        if (bucketSize <= 0) {
            throw new IllegalStateException("Bucket size is less or equal to zero.");
        }
        bucketCount = (int) Math.floor(intervalLength / bucketSize);
        if ((intervalLength - bucketSize * bucketCount) / intervalLength > 0.001) {
            bucketCount++;
        }

        double[] points = new double[bucketCount];
        points[0] = startPoint + bucketSize;
        points[points.length - 1] = endPoint - bucketSize;
        for (int i = 1; i < points.length - 1; i++) {
            points[i] = points[i - 1] + bucketSize;
        }

        return points;
    }

    protected String getTimeGroupFieldAliasPrefix() {
        return TIME_GROUP_FIELD_ALIAS_PREFIX;
    }

    protected String getHistogramGroupFieldAliasPrefix() {
        return HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX;
    }
}
