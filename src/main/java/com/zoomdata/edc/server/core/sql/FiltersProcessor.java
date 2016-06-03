/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.sql;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.DateTimeExpression;
import com.querydsl.core.types.dsl.NumberExpression;
import com.querydsl.core.types.dsl.StringExpression;
import com.zoomdata.edc.server.core.utils.SQLUtils;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterAND;
import com.zoomdata.gen.edc.filter.FilterEQ;
import com.zoomdata.gen.edc.filter.FilterEQI;
import com.zoomdata.gen.edc.filter.FilterFunction;
import com.zoomdata.gen.edc.filter.FilterGE;
import com.zoomdata.gen.edc.filter.FilterGT;
import com.zoomdata.gen.edc.filter.FilterIN;
import com.zoomdata.gen.edc.filter.FilterISNULL;
import com.zoomdata.gen.edc.filter.FilterLE;
import com.zoomdata.gen.edc.filter.FilterCONTAINS;
import com.zoomdata.gen.edc.filter.FilterLT;
import com.zoomdata.gen.edc.filter.FilterNOT;
import com.zoomdata.gen.edc.filter.FilterOR;
import com.zoomdata.gen.edc.types.FieldType;

import java.util.List;

public class FiltersProcessor {
    protected Path<?> table;
    protected List<Filter> thriftFilters;
    protected BooleanBuilder where;

    public Predicate process(Path<?> table, List<Filter> thriftFilters) {
        this.table = table;
        this.thriftFilters = thriftFilters;

        where = new BooleanBuilder();
        for (Filter filter : thriftFilters) {
            processFilter(table, filter, where);
        }
        return where;
    }

    public Path<?> getTable() {
        return table;
    }

    public List<Filter> getThriftFilters() {
        return thriftFilters;
    }

    public Predicate getWhere() {
        return where;
    }

    // ==================== FILTER IMPLEMENTATION ==========

    protected Predicate processFilter(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterFunction type = filter.getType();
        switch (type) {
            case AND: {
                processAND(table, filter, predicate);
                break;
            }
            case OR: {
                processOR(table, filter, predicate);
                break;
            }
            case NOT: {
                processNOT(table, filter, predicate);
                break;
            }
            case EQ: {
                processEQ(table, filter, predicate);
                break;
            }

            case LT: {
                processLT(table, filter, predicate);
                break;
            }

            case LE: {
                processLE(table, filter, predicate);
                break;
            }

            case GT: {
                processGT(table, filter, predicate);
                break;
            }

            case GE: {
                processGE(table, filter, predicate);
                break;
            }

            case EQI: {
                processEQI(table, filter, predicate);
                break;
            }

            case CONTAINS: {
                processCONTAINS(table, filter, predicate);
                break;
            }

            case IS_NULL: {
                processIS_NULL(table, filter, predicate);
                break;
            }

            case TEXT_SEARCH: {
                processTEXT_SEARCH(table, filter, predicate);
                break;
            }

            case IN: {
                processIN(table, filter, predicate);
                break;
            }
        }
        return predicate;
    }

    protected void processAND(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterAND filterAND = filter.getFilterAND();
        Predicate[] subfilters = filterAND.getFilters().stream()
            .map(subfilter -> processFilter(table, subfilter, new BooleanBuilder()))
            .toArray(Predicate[]::new);

        predicate.and(ExpressionUtils.allOf(subfilters));
    }

    protected void processOR(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterOR filterOR = filter.getFilterOR();
        Predicate[] subfilters = filterOR.getFilters().stream()
            .map(subfilter -> processFilter(table, subfilter, new BooleanBuilder()))
            .toArray(Predicate[]::new);

        predicate.and(ExpressionUtils.anyOf(subfilters));
    }

    protected void processNOT(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterNOT filterNOT = filter.getFilterNOT();
        Filter filterForNot = filterNOT.getFilter();
        predicate.andNot(processFilter(table, filterForNot, new BooleanBuilder()));
    }

    protected void processEQ(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterEQ filterEQ = filter.getFilterEQ();
        predicate.and(SQLUtils.createPredicateBinary(table, filterEQ.getType(), filterEQ.getValue(), filterEQ.getPath(),
                NumberExpression::eq, StringExpression::eq, DateTimeExpression::eq));
    }

    protected void processLT(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterLT filterLT = filter.getFilterLT();
        predicate.and(SQLUtils.createPredicateBinary(table, filterLT.getType(), filterLT.getValue(), filterLT.getPath(),
                NumberExpression::lt, StringExpression::lt, DateTimeExpression::lt));
    }

    protected void processLE(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterLE filterLE = filter.getFilterLE();
        predicate.and(SQLUtils.createPredicateBinary(table, filterLE.getType(), filterLE.getValue(), filterLE.getPath(),
                NumberExpression::loe, StringExpression::loe, DateTimeExpression::loe));
    }

    protected void processGT(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterGT filterGT = filter.getFilterGT();
        predicate.and(SQLUtils.createPredicateBinary(table, filterGT.getType(), filterGT.getValue(), filterGT.getPath(),
                NumberExpression::gt, StringExpression::gt, DateTimeExpression::gt));
    }

    protected void processGE(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterGE filterGE = filter.getFilterGE();
        predicate.and(SQLUtils.createPredicateBinary(table, filterGE.getType(), filterGE.getValue(), filterGE.getPath(),
                NumberExpression::goe, StringExpression::goe, DateTimeExpression::goe));
    }

    protected void processEQI(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterEQI filterEQI = filter.getFilterEQI();
        predicate.and(SQLUtils.createPredicateBinary(table, FieldType.STRING, filterEQI.getValue(), filterEQI.getPath(),
                null, StringExpression::equalsIgnoreCase, null));
    }

    protected void processCONTAINS(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterCONTAINS filterCONTAINS = filter.getFilterCONTAINS();
        predicate.and(SQLUtils.createPredicateBinary(table, FieldType.STRING, filterCONTAINS.getValue(), filterCONTAINS.getPath(),
                null, StringExpression::containsIgnoreCase, null));
    }

    protected void processIS_NULL(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterISNULL filterISNULL = filter.getFilterISNULL();
        predicate.and(SQLUtils.createPredicateUnary(table, filterISNULL.getType(), filterISNULL.getPath(),
                NumberExpression::isNull, StringExpression::isNull, DateTimeExpression::isNull));
    }

    protected void processTEXT_SEARCH(Path<?> table, Filter filter, BooleanBuilder predicate) {
        throw new UnsupportedOperationException("TEXT_SEARCH is not supported.");
    }

    protected void processIN(Path<?> table, Filter filter, BooleanBuilder predicate) {
        FilterIN filterIN = filter.getFilterIN();
        //noinspection unchecked
        predicate.and(SQLUtils.createPredicateVarary(table, filterIN.getType(), filterIN.getPath(), filterIN.getValues(),
                (nf, e) -> nf.in(e)));
    }
}
