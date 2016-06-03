/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.cratedb;

import com.querydsl.core.types.Ops;
import com.querydsl.sql.MySQLTemplates;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.edc.server.core.sql.ExtendedDateTimeOps;
import com.zoomdata.edc.server.core.utils.ThriftUtils;

import java.sql.Types;

/**
 * Extended template for MemSQL. All date related function for grouping are implemented
 */
public class CrateDbTemplates extends MySQLTemplates {
    public CrateDbTemplates() {
        this('\\', false);
    }

    public CrateDbTemplates(boolean quote) {
        this('\\', quote);
    }

    public CrateDbTemplates(char escape, boolean quote) {
        super(escape, quote);

        add(Ops.DateTimeOps.TRUNC_YEAR,   "DATE_TRUNC('year',{0})");
        //quarter function doesn't work in memsql, need to calculate
        add(ExtendedDateTimeOps.TRUNC_QUARTER,
                "cast(concat(date_format({0},'%Y-'), (3 * ceil(month({0})/3) - 2), '-1') as DATE)");
        add(Ops.DateTimeOps.TRUNC_MONTH,   "DATE_TRUNC('month',{0})");
        //can not parse custom date string, need to calculate. Start week day is Sunday
        add(Ops.DateTimeOps.TRUNC_WEEK,    "DATE_TRUNC('week',{0})");
        add(Ops.DateTimeOps.TRUNC_DAY,    "DATE_TRUNC('day',{0})");
        add(Ops.DateTimeOps.TRUNC_HOUR,    "DATE_TRUNC('hour',{0})");
        add(Ops.DateTimeOps.TRUNC_MINUTE,    "DATE_TRUNC('minute',{0})");
        add(Ops.DateTimeOps.TRUNC_SECOND,    "DATE_TRUNC('second',{0})");

        add(ExtendedDateTimeOps.FROM_UNIXTIME, "FROM_UNIXTIME({0})");
        add(ExtendedDateTimeOps.FROM_MILLIS, "FROM_UNIXTIME({0} / 1000)");
        add(ExtendedDateTimeOps.FROM_YEARPATTERN,"format('%s-01-01',{0})");
    }


    @Override
    public String serialize(String literal, int jdbcType){
        try {
            switch (jdbcType) {
                case Types.TIMESTAMP:
                case TIMESTAMP_WITH_TIMEZONE:
                    return "'" + ThriftUtils.GetSolrDateFromSqlDate(literal) + "'";
                // return "convert('" + literal + "', datetime)";
                case Types.DATE:
                    return "'" + ThriftUtils.GetSolrDateFromSqlDate(literal) + "'";

                // return "convert('" + literal + "', date)";
                case Types.TIME:
                case TIME_WITH_TIMEZONE:
                    return "'" + ThriftUtils.GetSolrDateFromSqlDate(literal) + "'";

                // return "convert('" + literal + "', time)";
                default:
                    return super.serialize(literal, jdbcType);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Builder builder() {
        return new Builder() {
            @Override
            protected SQLTemplates build(char escape, boolean quote) {
                return new CrateDbTemplates(escape, quote);
            }
        };
    }
}
