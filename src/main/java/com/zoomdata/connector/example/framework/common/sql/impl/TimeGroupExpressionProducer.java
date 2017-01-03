/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.querydsl.core.types.Operator;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.TemporalExpression;
import com.zoomdata.connector.example.framework.common.sql.ops.ExtendedDateTimeOps;
import com.zoomdata.gen.edc.group.TimeGroup;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldParams;
import com.zoomdata.gen.edc.types.FieldType;
import com.zoomdata.gen.edc.types.TimeGranularity;

import java.util.Date;
import java.util.Optional;

import static com.querydsl.core.types.dsl.Expressions.dateTimeOperation;
import static com.querydsl.core.types.dsl.Expressions.dateTimePath;
import static com.querydsl.core.types.dsl.Expressions.numberPath;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.SECONDS_FORMAT;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.YEAR_FORMAT;
import static java.util.Optional.ofNullable;

/**
 * TODO: introduce a separate format for milliseconds
 */
public class TimeGroupExpressionProducer {

    public TemporalExpression<Date> process(Path<?> table, TimeGroup group, FieldMetadata fieldMetadata) {
        Operator truncationOperator = defineTruncationOperator(group.getGranularity(), fieldMetadata);
        TemporalExpression<Date> pathToTimeField = createPathToTimeField(table, fieldMetadata);
        // TODO [oleksandr.chornyi]: Add verification that the target time group granularity makes sense for such a field
        if (isDateInYearFormat(fieldMetadata)) {
            return pathToTimeField;
        }
        return dateTimeOperation(Date.class, truncationOperator, pathToTimeField);
    }

    protected TemporalExpression<Date> createPathToTimeField(Path<?> table, FieldMetadata fieldMetadata) {
        if (fieldMetadata.getType() == FieldType.DATE) {
            return datePath(table, fieldMetadata);
        } else if (isDateInYearFormat(fieldMetadata)) {
            checkYearPatternGranularity(fieldMetadata);
            return yearPath(table, fieldMetadata);
        } else if (isDateInSecondsFormat(fieldMetadata)) {
            return secondsPath(table, fieldMetadata);
        } else {
            checkTimeGroupFieldType(fieldMetadata);
            return millisecondsPath(table, fieldMetadata);
        }
    }

    protected TemporalExpression<Date> datePath(Path<?> table, FieldMetadata fieldMetadata) {
        return dateTimePath(Date.class, table, fieldMetadata.getName());
    }

    protected TemporalExpression<Date> yearPath(Path<?> table, FieldMetadata fieldMetadata) {
        NumberPath<Long> yearFieldPath = numberPath(Long.class, table, fieldMetadata.getName());
        return dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_YEARPATTERN, yearFieldPath);
    }

    protected TemporalExpression<Date> secondsPath(Path<?> table, FieldMetadata fieldMetadata) {
        NumberPath<Long> unixtimeFieldPath = numberPath(Long.class, table, fieldMetadata.getName());
        return  dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_UNIXTIME, unixtimeFieldPath);
    }

    protected TemporalExpression<Date> millisecondsPath(Path<?> table, FieldMetadata fieldMetadata) {
        NumberPath<Long> millisFieldPath = numberPath(Long.class, table, fieldMetadata.getName());
        return dateTimeOperation(Date.class, ExtendedDateTimeOps.FROM_MILLIS, millisFieldPath);
    }


    private static boolean isDateInYearFormat(FieldMetadata fieldMetadata) {
        Optional<String> timeFieldPattern = getTimeFieldPattern(fieldMetadata);
        return timeFieldPattern.isPresent() && YEAR_FORMAT.equals(timeFieldPattern.get());
    }

    private static boolean isDateInSecondsFormat(FieldMetadata fieldMetadata) {
        return getTimeFieldPattern(fieldMetadata).filter(SECONDS_FORMAT::equals).isPresent();
    }

    private static Optional<String> getTimeFieldPattern(FieldMetadata fieldMetadata) {
        return ofNullable(fieldMetadata.getFieldParams()).map(FieldParams::getTimeFieldPattern);
    }

    protected Operator defineTruncationOperator(TimeGranularity granularity, FieldMetadata fieldMetadata) {
        switch (granularity) {
            case MILLISECOND: {
                throw new UnsupportedOperationException(granularity + " time granularity is not supported");
            }
            case SECOND: {
                return Ops.DateTimeOps.TRUNC_SECOND;
            }
            case MINUTE: {
                return Ops.DateTimeOps.TRUNC_MINUTE;
            }
            case HOUR: {
                return Ops.DateTimeOps.TRUNC_HOUR;
            }
            case DAY: {
                return Ops.DateTimeOps.TRUNC_DAY;
            }
            case WEEK: {
                return Ops.DateTimeOps.TRUNC_WEEK;
            }
            case MONTH: {
                return Ops.DateTimeOps.TRUNC_MONTH;
            }
            case QUARTER: {
                return ExtendedDateTimeOps.TRUNC_QUARTER;
            }
            case YEAR: {
                return Ops.DateTimeOps.TRUNC_YEAR;
            }
            default: {
                throw new IllegalStateException("Time group with granularity " + granularity + " is not supported.");
            }
        }
    }

    private static void checkYearPatternGranularity(FieldMetadata fieldMetadata) {
        if (fieldMetadata.getFieldParams().getTimeFieldGranularity() != TimeGranularity.YEAR) {
            throw new IllegalArgumentException("Year column can't have granularity " +
                    fieldMetadata.getFieldParams().getTimeFieldGranularity());
        }
    }

    private static void checkTimeGroupFieldType(FieldMetadata fieldMetadata) {
        if (fieldMetadata.getType() == FieldType.STRING) {
            throw new IllegalArgumentException("Unable to create a time group using a field of type String: " + fieldMetadata);
        }
    }

}

