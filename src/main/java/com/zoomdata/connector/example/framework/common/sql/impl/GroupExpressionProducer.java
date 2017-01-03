/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.common.sql.impl;

import com.google.common.base.Supplier;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.ComparableExpressionBase;
import com.querydsl.core.types.dsl.NumberPath;
import com.querydsl.core.types.dsl.NumberTemplate;
import com.querydsl.core.types.dsl.StringExpression;
import com.querydsl.core.types.dsl.TemporalExpression;
import com.zoomdata.connector.example.common.utils.struct.Pair;
import com.zoomdata.connector.example.framework.common.sql.IAliasGenerator;
import com.zoomdata.connector.example.framework.common.sql.IGroupExpressionProducer;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.group.GroupType;
import com.zoomdata.gen.edc.group.HistogramGroup;
import com.zoomdata.gen.edc.group.TimeGroup;
import com.zoomdata.gen.edc.types.FieldMetadata;
import org.springframework.util.Assert;

import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.querydsl.core.types.dsl.Expressions.cases;
import static com.querydsl.core.types.dsl.Expressions.dateTimePath;
import static com.querydsl.core.types.dsl.Expressions.numberPath;
import static com.querydsl.core.types.dsl.Expressions.numberTemplate;
import static com.querydsl.core.types.dsl.Expressions.stringPath;
import static com.zoomdata.connector.example.common.utils.CollectionUtils.toSpecificMap;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

public final class GroupExpressionProducer implements IGroupExpressionProducer {

    public static final String HISTOGRAM_MSG_START_GREATER_END =
            "Start point of histogram group is greater then end point.";
    public static final String HISTOGRAM_MSG_NEGATIVE_BUCKET_SIZE = "Bucket size is less or equal to zero.";
    public static final String HISTOGRAM_MSG_LESS_THAN_ONE_BUCKET = "Histogram should have more then one bucket!";

    public static final String HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX = "__hg_";
    public static final String TIME_GROUP_FIELD_ALIAS_PREFIX = "__tg_";
    public static final String HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX_WHICH_STARTS_WITH_LETTER = "hg_";
    public static final String TIME_GROUP_FIELD_ALIAS_PREFIX_WHICH_STARTS_WITH_LETTER = "tg_";

    private Path<?> table;
    private List<Group> thriftGroups;
    private Map<Group, AliasedComparableExpressionBase> groupExpressions;

    private String histogramGroupFieldAliasPrefix = HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX;
    private String timeGroupFieldAliasPrefix = TIME_GROUP_FIELD_ALIAS_PREFIX;

    private boolean useAliasInsteadOfOriginalExpressionInGroupByClause = true;
    private boolean useAliasInsteadOfOriginalExpressionInOrderByClause = true;

    private IAliasGenerator aliasGenerator;
    private Supplier<AliasGenerator> aliasGeneratorSupplier = () -> AliasGenerator.newAliasGenerator().build();
    private TimeGroupExpressionProducer timeGroupExpressionProducer = new TimeGroupExpressionProducer();

    public GroupExpressionProducer() {
    }

    public GroupExpressionProducer useOriginalExpressionInGroupByClause() {
        this.useAliasInsteadOfOriginalExpressionInGroupByClause = false;
        return this;
    }

    public GroupExpressionProducer useOriginalExpressionInOrderByClause() {
        this.useAliasInsteadOfOriginalExpressionInOrderByClause = false;
        return this;
    }

    public GroupExpressionProducer withHistogramGroupFieldAliasPrefix(String prefix) {
        this.histogramGroupFieldAliasPrefix = prefix;
        return this;
    }

    public GroupExpressionProducer withTimeGroupFieldAliasPrefix(String prefix) {
        this.timeGroupFieldAliasPrefix = prefix;
        return this;
    }

    public GroupExpressionProducer withHistogramAndTimeGroupFieldAliasPrefixStartingWithLetter() {
        return this
                .withHistogramGroupFieldAliasPrefix(HISTOGRAM_GROUP_FIELD_ALIAS_PREFIX_WHICH_STARTS_WITH_LETTER)
                .withTimeGroupFieldAliasPrefix(TIME_GROUP_FIELD_ALIAS_PREFIX_WHICH_STARTS_WITH_LETTER);
    }

    public GroupExpressionProducer useAliasGeneratorSupplier(Supplier<AliasGenerator> supplier) {
        this.aliasGeneratorSupplier = supplier;
        return this;
    }

    public IGroupExpressionProducer withTimeGroupExpressionProducer(TimeGroupExpressionProducer timeGroupExpressionProducer) {
        this.timeGroupExpressionProducer = timeGroupExpressionProducer;
        return this;
    }

    // ==================== INTERFACE IMPLEMENTATION ====================

    @Override
    public GroupExpressionProducer
    process(
            Path<?> table,
            List<Group> groups,
            Map<String, FieldMetadata> fieldMetadata
    ) {
        aliasGenerator = aliasGeneratorSupplier.get();
        this.table = table;
        this.thriftGroups = groups;
        this.groupExpressions = ofNullable(groups).map(gr -> gr.stream()
                .map(g -> transformGroupToExpression(g, fieldMetadata))
                .collect(
                        toSpecificMap(Pair::getLeft, Pair::getRight, LinkedHashMap::new)
                )
        ).orElse(null);
        return this;
    }

    @Override
    public List<Group> getThriftGroups() {
        return thriftGroups;
    }

    @Override
    public List<AliasedComparableExpressionBase> getExpressionsForSelect() {
        return getExpressionsAsList();
    }

    @Override
    public List<AliasedComparableExpressionBase> getExpressionsForGroupBy() {
        return useAliasInsteadOfOriginalExpressionInGroupByClause ? getExpressionsAsList() :
                mapGroupExpressions(expressions -> transformExpressionMapToList(
                        expressions,
                        AliasedComparableExpressionBase::withoutAlias
                        )
                );
    }

    @Override
    public AliasedComparableExpressionBase getExpressionForOrderBy(Group group) {
        return mapGroupExpressions((Map<Group, AliasedComparableExpressionBase> expressions) -> getExpression(
                expressions,
                group,
                useAliasInsteadOfOriginalExpressionInOrderByClause
                )
        );
    }


    // ==================== GROUPS IMPLEMENTATION ====================

    private Pair<Group, AliasedComparableExpressionBase> transformGroupToExpression(
            Group g, Map<String,
            FieldMetadata> fieldMetadata
    ) {

        AliasedComparableExpressionBase e;
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
                e = processTIME_GROUP(g, fieldMetadata);
                break;
            }
            default: {
                throw new IllegalStateException("Group of type " + type + " is not supported.");
            }
        }
        return Pair.create(g, e);
    }

    private AliasedComparableExpressionBase processATTRIBUTE_GROUP(
            Path<?> table,
            Group g,
            Map<String, FieldMetadata> fieldMetadata
    ) {
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
            // TODO [oleksandr.chornyi]: What to do with UNKNOWN type?
            default:
                throw new IllegalArgumentException("Type mapping not found for type: " + metadata.getType());
        }
        return AliasedComparableExpressionBase.create(path, aliasGenerator.generate(field));
    }

    private AliasedComparableExpressionBase processHISTOGRAM_GROUP(Group g) {
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

        return AliasedComparableExpressionBase.create(
                expression,
                aliasGenerator.generate(histogramGroupFieldAliasPrefix + Utils.underscore(group.getField()))
        );
    }

    // TODO can we make this class private?
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

        BigDecimal pointCountDecimal = endPoint.subtract(startPoint).divide(bucketSize, 0, BigDecimal.ROUND_DOWN);
        int pointCount = pointCountDecimal.intValue();
        //exclude point which equals to end point
        if(pointCountDecimal.multiply(bucketSize).add(startPoint).compareTo(endPoint) == 0){
            pointCount--;
        }
        return IntStream.range(0, pointCount)
                .mapToDouble(i -> startPoint.add(bucketSize.multiply(new BigDecimal(i + 1))).doubleValue())
                .toArray();
    }

    private String printHistogram(HistogramGroup histogram) {
        return format(" Start: %f, end: %f, bucketSize: %f", histogram.getStartPoint(), histogram.getEndPoint(),
                histogram.getBucketSize());
    }

    private AliasedComparableExpressionBase processTIME_GROUP(Group g, Map<String, FieldMetadata> metadata) {
        TimeGroup timeGroup = g.getTimeGroup();
        FieldMetadata fieldMetadata = metadata.get(timeGroup.getField());
        Assert.isTrue(timeGroup.getField().equals(fieldMetadata.getName()),
                "FieldMetadata name and TimeGroup name doesn't match: " + fieldMetadata.getName() + " vs " + timeGroup.getField());
        TemporalExpression<?> groupExpression = timeGroupExpressionProducer.process(table, timeGroup, fieldMetadata);

        return AliasedComparableExpressionBase.create(
                groupExpression,
                aliasGenerator.generate(
                        timeGroupFieldAliasPrefix + timeGroup.getGranularity().name().toLowerCase() + "_over_" +
                                Utils.underscore(timeGroup.getField())
                )
        );
    }

    public static NumberTemplate<Double> doubleTemplate(double val) {
        return numberTemplate(Double.class, String.valueOf(val));
    }

    private <T> T mapGroupExpressions(Function<Map<Group, AliasedComparableExpressionBase>, T> f) {
        return ofNullable(groupExpressions).map(f).orElse(null);
    }

    private List<AliasedComparableExpressionBase> transformExpressionMapToList(
            Map<Group, AliasedComparableExpressionBase> expressions,
            Function<AliasedComparableExpressionBase, AliasedComparableExpressionBase> f
    ) {
        return expressions.values().stream().map(f).collect(Collectors.toList());
    }

    private List<AliasedComparableExpressionBase> getExpressionsAsList() {
        return mapGroupExpressions(expressions -> transformExpressionMapToList(expressions, Function.identity()));
    }

    private AliasedComparableExpressionBase getExpression(
            Map<Group, AliasedComparableExpressionBase> expressions,
            Group group,
            boolean withAlias
    ) {
        AliasedComparableExpressionBase expression = expressions.get(group);
        return withAlias ? expression : expression.withoutAlias();
    }

}