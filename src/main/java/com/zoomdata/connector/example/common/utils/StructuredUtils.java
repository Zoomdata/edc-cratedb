/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */

package com.zoomdata.connector.example.common.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.zoomdata.gen.edc.filter.Filter;
import com.zoomdata.gen.edc.filter.FilterAND;
import com.zoomdata.gen.edc.filter.FilterCONTAINS;
import com.zoomdata.gen.edc.filter.FilterEQ;
import com.zoomdata.gen.edc.filter.FilterEQI;
import com.zoomdata.gen.edc.filter.FilterFunction;
import com.zoomdata.gen.edc.filter.FilterGE;
import com.zoomdata.gen.edc.filter.FilterGT;
import com.zoomdata.gen.edc.filter.FilterIN;
import com.zoomdata.gen.edc.filter.FilterISNULL;
import com.zoomdata.gen.edc.filter.FilterLE;
import com.zoomdata.gen.edc.filter.FilterLT;
import com.zoomdata.gen.edc.filter.FilterNOT;
import com.zoomdata.gen.edc.filter.FilterOR;
import com.zoomdata.gen.edc.filter.FilterTEXT_SEARCH;
import com.zoomdata.gen.edc.group.AttributeGroup;
import com.zoomdata.gen.edc.group.Group;
import com.zoomdata.gen.edc.group.GroupType;
import com.zoomdata.gen.edc.group.HistogramGroup;
import com.zoomdata.gen.edc.group.TimeGroup;
import com.zoomdata.gen.edc.metric.Metric;
import com.zoomdata.gen.edc.metric.MetricAvg;
import com.zoomdata.gen.edc.metric.MetricCalc;
import com.zoomdata.gen.edc.metric.MetricCount;
import com.zoomdata.gen.edc.metric.MetricDistinctCount;
import com.zoomdata.gen.edc.metric.MetricLastValue;
import com.zoomdata.gen.edc.metric.MetricMax;
import com.zoomdata.gen.edc.metric.MetricMin;
import com.zoomdata.gen.edc.metric.MetricPercentile;
import com.zoomdata.gen.edc.metric.MetricSum;
import com.zoomdata.gen.edc.metric.MetricType;
import com.zoomdata.gen.edc.request.AggDataRequest;
import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.request.DataReadRequest;
import com.zoomdata.gen.edc.request.DistinctValuesRequest;
import com.zoomdata.gen.edc.request.RawDataRequest;
import com.zoomdata.gen.edc.request.StatField;
import com.zoomdata.gen.edc.request.StatFunction;
import com.zoomdata.gen.edc.request.StatsDataRequest;
import com.zoomdata.gen.edc.request.StructuredRequest;
import com.zoomdata.gen.edc.request.StructuredRequestType;
import com.zoomdata.gen.edc.request.serverdescription.ConnectionParameter;
import com.zoomdata.gen.edc.request.serverdescription.ParameterEnum;
import com.zoomdata.gen.edc.request.serverdescription.ParameterInteger;
import com.zoomdata.gen.edc.request.serverdescription.ParameterString;
import com.zoomdata.gen.edc.request.serverdescription.ParameterType;
import com.zoomdata.gen.edc.sort.AggSort;
import com.zoomdata.gen.edc.sort.RawSort;
import com.zoomdata.gen.edc.sort.SortDir;
import com.zoomdata.gen.edc.sort.SortType;
import com.zoomdata.gen.edc.types.Field;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldParams;
import com.zoomdata.gen.edc.types.FieldType;
import com.zoomdata.gen.edc.types.PartitionParams;
import com.zoomdata.gen.edc.types.TimeGranularity;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.zoomdata.connector.example.common.utils.FieldMetaFlag.PARTITION;
import static com.zoomdata.connector.example.common.utils.FieldMetaFlag.PLAYABLE;
import static com.zoomdata.connector.example.common.utils.FileUtils.readFileFromClassPath;
import static com.zoomdata.connector.example.common.utils.metadatabuilders.Filters.isNull;
import static com.zoomdata.connector.example.common.utils.metadatabuilders.Filters.not;
import static com.zoomdata.connector.example.common.utils.metadatabuilders.Groups.group;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

public final class StructuredUtils {
    private StructuredUtils() { }

    private static final Map<String, Function<JSONObject, Filter>> FILTERS_MAP =
            ImmutableMap.<String, Function<JSONObject, Filter>>builder()
                    .put("and", StructuredUtils::processFilterAND)
                    .put("or", StructuredUtils::processFilterOR)
                    .put("not", StructuredUtils::processFilterNOT)
                    .put("eq", StructuredUtils::processFilterEQ)
                    .put("eqi", StructuredUtils::processFilterEQI)
                    .put("in", StructuredUtils::processFilterIN)
                    .put("ge", StructuredUtils::processFilterGE)
                    .put("gt", StructuredUtils::processFilterGT)
                    .put("le", StructuredUtils::processFilterLE)
                    .put("lt", StructuredUtils::processFilterLT)
                    .put("contains", StructuredUtils::processFilterCONTAINS)
                    .put("is_null", StructuredUtils::processFilterISNULL)
                    .put("text_search", StructuredUtils::processFilterTEXTSEARCH)
                    .build();

    private static final Map<String, BiFunction<JSONObject, StructuredRequest, StructuredRequest>> STRUCTURED_MAP =
            ImmutableMap.<String, BiFunction<JSONObject, StructuredRequest, StructuredRequest>>builder()
                    .put("raw", StructuredUtils::readRaw)
                    .put("aggregated", StructuredUtils::readAggregated)
                    .put("stats", StructuredUtils::readStats)
                    .put("distinct_values", StructuredUtils::readDistinctValues)
                    .build();

    private static final Map<String, Function<JSONObject, Metric>> METRICS_MAP =
            ImmutableMap.<String, Function<JSONObject, Metric>>builder()
                    .put("sum", StructuredUtils::processMetricSUM)
                    .put("min", StructuredUtils::processMetricMIN)
                    .put("max", StructuredUtils::processMetricMAX)
                    .put("avg", StructuredUtils::processMetricAVG)
                    .put("count", StructuredUtils::processMetricCOUNT)
                    .put("distinct_count", StructuredUtils::processMetricDISTINCTCOUNT)
                    .put("last_value", StructuredUtils::processMetricLASTVALUE)
                    .put("percentiles", StructuredUtils::processMetricPERCENTILES)
                    .put("calculation", StructuredUtils::processMetricCALCULATION)
                    .build();

    private static final Map<String, Function<JSONObject, Group>> GROUP_MAP =
            ImmutableMap.<String, Function<JSONObject, Group>>builder()
                    .put("attribute_group", StructuredUtils::processAttributeGroup)
                    .put("time_group", StructuredUtils::processTimeGroup)
                    .put("histogram_group", StructuredUtils::processHistogramGroup)
                    .build();

    public static StructuredRequest parseToStructured(JSONObject rootJson, CollectionInfo colInfo) {
        StructuredRequest structuredRequest = new StructuredRequest();
        structuredRequest.setCollectionInfo(colInfo);
        if (rootJson.has("fieldMetadata")) {
            structuredRequest.setFieldMetadata(parseMetaData(rootJson.getJSONObject("fieldMetadata")));
        }
        String type = StringUtils.toLowerOrNull(rootJson.getString("type"));
        BiFunction<JSONObject, StructuredRequest, StructuredRequest> parseFn = STRUCTURED_MAP.get(type);
        if (parseFn == null) {
            throw new IllegalArgumentException("Unsupported structured type: " + type);
        } else {
            return parseFn.apply(rootJson, structuredRequest);
        }
    }

    public static DataReadRequest checkAndReplaceDistinctValuesRequestWithAggDataRequest(DataReadRequest request) {
        if (request.isSetStructured() && request.getStructured().isSetDistinctValuesRequest()) {
            StructuredRequest structured = request.getStructured();
            DistinctValuesRequest distinctValuesRequest = structured.getDistinctValuesRequest();
            Map<String, FieldMetadata> fieldMetadata = structured.getFieldMetadata();
            DataReadRequest aggRequest = request.deepCopy();
            aggRequest.getStructured().setType(StructuredRequestType.AGG);
            aggRequest.getStructured().setAggDataRequest(convertToAggRequest(distinctValuesRequest, fieldMetadata));
            return aggRequest;
        }
        return request;
    }

    public static AggDataRequest convertToAggRequest(DistinctValuesRequest distinctValuesRequest, Map<String, FieldMetadata> fieldMetadata) {
        AggDataRequest aggDataRequest = new AggDataRequest();
        String field = distinctValuesRequest.getField();
        Group group = group(field);
        aggDataRequest.setGroups(asList(group));

        FieldType type = fieldMetadata.get(field).getType();
        Filter isNotNullFilter = not(isNull(field, type));
        List<Filter> filters = Lists.newArrayList(isNotNullFilter);
        if (distinctValuesRequest.isSetFilters()) {
            filters.addAll(distinctValuesRequest.getFilters());
        }
        aggDataRequest.setFilters(filters);

        if (distinctValuesRequest.isSetLimit()) {
            aggDataRequest.setLimit(distinctValuesRequest.getLimit());
        }
        if (distinctValuesRequest.isSetOffset()) {
            aggDataRequest.setOffset(distinctValuesRequest.getOffset());
        }
        SortDir sortDir = distinctValuesRequest.isSetSort()
                ? distinctValuesRequest.getSort().getDirection()
                : SortDir.ASC;
        aggDataRequest.setSorts(Collections.singletonList(
                new AggSort(SortType.GROUP)
                        .setDirection(sortDir)
                        .setGroup(group)
        ));
        return aggDataRequest;
    }

    private static StructuredRequest readAggregated(JSONObject json, StructuredRequest request) {
        AggDataRequest aggDataRequest = new AggDataRequest();
        aggDataRequest.setGroups(parseGroups(json));
        aggDataRequest.setMetrics(parseMetrics(json));
        aggDataRequest.setFilters(parseFilters(json));
        aggDataRequest.setSorts(parseAggSorts(json));

        Integer limit = parseLimit(json);
        if (limit != null) {
            aggDataRequest.setLimit(limit);
        }

        Integer offset = parseOffset(json);
        if (offset != null) {
            aggDataRequest.setOffset(offset);
        }

        request.setType(StructuredRequestType.AGG);
        request.setAggDataRequest(aggDataRequest);
        return request;
    }

    private static List<Group> parseGroups(JSONObject json) {
        List<Group> groups = new ArrayList<>();
        if (json.has("groups")) {
            JSONArray groupsArray = json.getJSONArray("groups");
            if (groupsArray != null) {
                for (int i = 0; i < groupsArray.length(); i++) {
                    JSONObject groupJson = groupsArray.getJSONObject(i);
                    String type = StringUtils.toLowerOrNull(groupJson.getString("type"));
                    Function<JSONObject, Group> group = GROUP_MAP.get(type);
                    if (group == null) {
                        throw new IllegalArgumentException("Invalid group: " + type);
                    } else {
                        groups.add(group.apply(groupJson));
                    }
                }
            }
        }
        return groups;
    }

    private static Group processAttributeGroup(JSONObject json) {
        Group group = new Group();
        group.setType(GroupType.ATTRIBUTE_GROUP);
        group.setAttributeGroup(new AttributeGroup().setField(json.getString("field")));
        return group;
    }

    private static Group processTimeGroup(JSONObject json) {
        Group group = new Group();
        group.setType(GroupType.TIME_GROUP);
        group.setTimeGroup(new TimeGroup()
                .setField(json.getString("field"))
                .setGranularity(TimeGranularity.valueOf(json.getString("granularity"))));
        return group;
    }

    private static Group processHistogramGroup(JSONObject json) {
        Group group = new Group();
        group.setType(GroupType.HISTOGRAM_GROUP);
        group.setHistogramGroup(new HistogramGroup()
                .setField(json.getString("field"))
                .setBucketSize(json.getDouble("bucket_size"))
                .setStartPoint(json.getDouble("start_point"))
                .setEndPoint(json.getDouble("end_point"))
        );
        return group;
    }

    private static List<Metric> parseMetrics(JSONObject json) {
        JSONArray metricsArray = json.getJSONArray("metrics");
        List<Metric> metrics = new ArrayList<>();
        if (metricsArray != null) {
            for (int i = 0; i < metricsArray.length(); i++) {
                JSONObject metricJson = metricsArray.getJSONObject(i);
                String type = StringUtils.toLowerOrNull(metricJson.getString("type"));
                Function<JSONObject, Metric> parseFn = METRICS_MAP.get(type);
                if (parseFn == null) {
                    throw new IllegalArgumentException("Invalid metric type: " + type);
                } else {
                    metrics.add(parseFn.apply(metricJson));
                }
            }
        }
        return metrics;
    }

    private static Metric processMetricSUM(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.SUM);
        metric.setSum(new MetricSum().setField(json.getString("field")));
        return metric;
    }

    private static Metric processMetricMIN(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.MIN);
        metric.setMin(new MetricMin().setField(json.getString("field")));
        return metric;
    }

    private static Metric processMetricMAX(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.MAX);
        metric.setMax(new MetricMax().setField(json.getString("field")));
        return metric;
    }

    private static Metric processMetricAVG(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.AVG);
        metric.setAvg(new MetricAvg().setField(json.getString("field")));
        return metric;
    }

    private static Metric processMetricCOUNT(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.COUNT);
        MetricCount metricCount = new MetricCount();
        if (json.has("field")) {
            metricCount.setField(json.getString("field"));
        }
        metric.setCount(metricCount);
        return metric;
    }

    private static Metric processMetricLASTVALUE(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.LAST_VALUE);
        metric.setLastValue(new MetricLastValue()
                .setField(json.getString("field"))
                .setTimeField(json.getString("time_field"))
        );
        return metric;
    }

    private static Metric processMetricDISTINCTCOUNT(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.DISTINCT_COUNT);
        metric.setDistinctCount(new MetricDistinctCount()
                .setField(json.getString("field"))
        );
        return metric;
    }

    private static Metric processMetricPERCENTILES(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.PERCENTILES);
        MetricPercentile metricPercentiles = new MetricPercentile();
        metricPercentiles.setField(json.getString("field"));
        metricPercentiles.setMargin(json.getDouble("margin"));
        metric.setPercentile(metricPercentiles);
        return metric;
    }

    private static Metric processMetricCALCULATION(JSONObject json) {
        Metric metric = new Metric();
        metric.setType(MetricType.CALC);
        metric.setCalculation(new MetricCalc().setValue(json.getString("value")));
        return metric;
    }

    private static StructuredRequest readDistinctValues(JSONObject json, StructuredRequest structuredRequest) {
        DistinctValuesRequest distinctValuesRequest = new DistinctValuesRequest();
        distinctValuesRequest.setField(parseField(json));
        distinctValuesRequest.setFilters(parseFilters(json));
        Integer limit = parseLimit(json);
        if (limit != null) {
            distinctValuesRequest.setLimit(limit);
        }

        Integer offset = parseOffset(json);
        if (offset != null) {
            distinctValuesRequest.setOffset(offset);
        }

        structuredRequest.setType(StructuredRequestType.DISTINCT_VALUES);
        structuredRequest.setDistinctValuesRequest(distinctValuesRequest);
        return structuredRequest;
    }

    private static StructuredRequest readRaw(JSONObject json, StructuredRequest request) {
        RawDataRequest rawDataRequest = new RawDataRequest();


        rawDataRequest.setFields(parseFields(json));
        rawDataRequest.setFilters(parseFilters(json));
        rawDataRequest.setSorts(parseRawSorts(json));

        Integer limit = parseLimit(json);
        if (limit != null) {
            rawDataRequest.setLimit(limit);
        }

        Integer offset = parseOffset(json);
        if (offset != null) {
            rawDataRequest.setOffset(offset);
        }

        request.setType(StructuredRequestType.RAW);
        request.setRawDataRequest(rawDataRequest);
        return request;
    }

    private static StructuredRequest readStats(JSONObject json, StructuredRequest request) {
        StatsDataRequest statsDataRequest = new StatsDataRequest();
        statsDataRequest.setStatFields(parseStatFields(json));
        request.setType(StructuredRequestType.STATS);
        request.setStatsDataRequest(statsDataRequest);
        return request;
    }

    private static String parseField(JSONObject json) {
        return json.getString("field");
    }

    private static List<StatField> parseStatFields(JSONObject json) {
        JSONArray statFieldsArray = json.getJSONArray("stat_fields");
        if (statFieldsArray == null || statFieldsArray.length() == 0) {
            throw new IllegalArgumentException("Field 'stat_fields' is required");
        }
        List<StatField> statFields = new ArrayList<>();
        for (int i = 0; i < statFieldsArray.length(); i++) {
            String field = statFieldsArray.getJSONObject(i).getString("field");
            String statFunction = statFieldsArray.getJSONObject(i).getString("function");
            statFields.add(new StatField(field, StatFunction.valueOf(statFunction)));
        }
        return statFields;
    }

    private static List<RawSort> parseRawSorts(JSONObject json) {
        List<RawSort> sorts = new ArrayList<>();
        if (json.has("sorts")) {
            JSONArray sortsArray = json.getJSONArray("sorts");
            if (sortsArray != null && sortsArray.length() > 0) {
                for (int i = 0; i < sortsArray.length(); i++) {
                    sorts.add(processJsonRawSort(sortsArray.getJSONObject(i)));
                }
            }
        }
        return sorts;
    }

    private static RawSort processJsonRawSort(JSONObject jsonObject) {
        RawSort sort = new RawSort();
        sort.setField(jsonObject.getString("field"));
        sort.setDirection(SortDir.valueOf(StringUtils.toUpperOrNull(jsonObject.getString("dir"))));
        return sort;
    }

    private static List<AggSort> parseAggSorts(JSONObject json) {
        List<AggSort> sorts = new ArrayList<>();
        if (json.has("sorts")) {
            JSONArray sortsArray = json.getJSONArray("sorts");
            if (sortsArray != null && sortsArray.length() > 0) {
                for (int i = 0; i < sortsArray.length(); i++) {
                    sorts.add(processJsonAggSort(sortsArray.getJSONObject(i)));
                }
            }
        }
        return sorts;
    }

    private static AggSort processJsonAggSort(JSONObject jsonObject) {
        AggSort sort = new AggSort();
        sort.setType(SortType.valueOf(jsonObject.getString("type")));
        sort.setDirection(SortDir.valueOf(StringUtils.toUpperOrNull(jsonObject.getString("dir"))));

        if (sort.getType() == SortType.GROUP) {
            JSONObject groupJson = jsonObject.getJSONObject("group");
            String type = StringUtils.toLowerOrNull(groupJson.getString("type"));
            Function<JSONObject, Group> group = GROUP_MAP.get(type);
            if (group == null) {
                throw new IllegalArgumentException("Unsuported group type: " + type);
            } else {
                sort.setGroup(group.apply(groupJson));
            }
        } else {
            JSONObject metricJson = jsonObject.getJSONObject("metric");
            String type = StringUtils.toLowerOrNull(metricJson.getString("type"));
            Function<JSONObject, Metric> parseFn = METRICS_MAP.get(type);
            if (parseFn == null) {
                throw new IllegalArgumentException("Unsupported metric type: " + type);
            } else {
                sort.setMetric(parseFn.apply(metricJson));
            }
        }

        return sort;
    }

    private static List<Filter> parseFilters(JSONObject json) {
        List<Filter> filters = new ArrayList<>();
        if (json.has("filters")) {
            JSONArray filtersArray = json.getJSONArray("filters");
            if (filtersArray != null && filtersArray.length() > 0) {
                for (int i = 0; i < filtersArray.length(); i++) {
                    filters.add(processJsonFilter(filtersArray.getJSONObject(i)));
                }
            }
        }
        return filters;
    }

    private static Filter processJsonFilter(JSONObject jsonFilter) {
        String type = jsonFilter.getString("type") == null ? null : jsonFilter.getString("type").toLowerCase();
        Function<JSONObject, Filter> filterParseFN = FILTERS_MAP.get(type);
        if (filterParseFN == null) {
            throw new IllegalArgumentException("Unsuported filter type: " + type);
        } else {
            return filterParseFN.apply(jsonFilter);
        }
    }

    private static Filter processFilterIN(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterIN filterIN = new FilterIN();

        filterIN.setPath(jsonFilter.getString("field_path"));
        filterIN.setType(FieldType.valueOf(jsonFilter.getString("field_type")));

        JSONArray filterValues = jsonFilter.getJSONArray("values");
        if (filterValues != null) {
            List<Field> values = new ArrayList<>();
            for (int i = 0; i < filterValues.length(); i++) {
                values.add(new Field().setValue(filterValues.getString(i)));
            }
            filterIN.setValues(values);
        }
        filter.setType(FilterFunction.IN);
        filter.setFilterIN(filterIN);
        return filter;
    }

    private static Filter processFilterEQ(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterEQ filterEQ = new FilterEQ();

        filterEQ.setPath(jsonFilter.getString("field_path"));
        filterEQ.setType(FieldType.valueOf(jsonFilter.getString("field_type")));
        filterEQ.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.EQ);
        filter.setFilterEQ(filterEQ);
        return filter;
    }

    private static Filter processFilterISNULL(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterISNULL filterISNULL = new FilterISNULL();

        filterISNULL.setPath(jsonFilter.getString("field_path"));
        filterISNULL.setType(FieldType.valueOf(jsonFilter.getString("field_type")));

        filter.setType(FilterFunction.IS_NULL);
        filter.setFilterISNULL(filterISNULL);
        return filter;
    }

    private static Filter processFilterCONTAINS(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterCONTAINS filterCONTAINS = new FilterCONTAINS();

        filterCONTAINS.setPath(jsonFilter.getString("field_path"));
        filterCONTAINS.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.CONTAINS);
        filter.setFilterCONTAINS(filterCONTAINS);
        return filter;
    }

    private static Filter processFilterTEXTSEARCH(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterTEXT_SEARCH filterTS = new FilterTEXT_SEARCH();

        filterTS.setPath(jsonFilter.getString("field_path"));
        filterTS.setValue(jsonFilter.getString("value"));

        filter.setType(FilterFunction.TEXT_SEARCH);
        filter.setFilterTEXT_SEARCH(filterTS);
        return filter;
    }

    private static Filter processFilterEQI(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterEQI filterEQI = new FilterEQI();

        filterEQI.setPath(jsonFilter.getString("field_path"));
        filterEQI.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.EQI);
        filter.setFilterEQI(filterEQI);
        return filter;
    }

    private static Filter processFilterGE(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterGE filterGE = new FilterGE();

        filterGE.setPath(jsonFilter.getString("field_path"));
        filterGE.setType(FieldType.valueOf(jsonFilter.getString("field_type")));
        filterGE.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.GE);
        filter.setFilterGE(filterGE);
        return filter;
    }

    private static Filter processFilterGT(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterGT filterGT = new FilterGT();

        filterGT.setPath(jsonFilter.getString("field_path"));
        filterGT.setType(FieldType.valueOf(jsonFilter.getString("field_type")));
        filterGT.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.GT);
        filter.setFilterGT(filterGT);
        return filter;
    }

    private static Filter processFilterLT(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterLT filterLT = new FilterLT();

        filterLT.setPath(jsonFilter.getString("field_path"));
        filterLT.setType(FieldType.valueOf(jsonFilter.getString("field_type")));
        filterLT.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.LT);
        filter.setFilterLT(filterLT);
        return filter;
    }

    private static Filter processFilterLE(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterLE filterLE = new FilterLE();

        filterLE.setPath(jsonFilter.getString("field_path"));
        filterLE.setType(FieldType.valueOf(jsonFilter.getString("field_type")));
        filterLE.setValue(new Field().setValue(jsonFilter.getString("value")));

        filter.setType(FilterFunction.LE);
        filter.setFilterLE(filterLE);
        return filter;
    }

    private static Filter processFilterNOT(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterNOT filterNOT = new FilterNOT();
        filterNOT.setFilter(processJsonFilter(jsonFilter.getJSONObject("filter")));
        filter.setType(FilterFunction.NOT);
        filter.setFilterNOT(filterNOT);
        return filter;
    }

    private static Filter processFilterAND(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterAND filterAND = new FilterAND();
        List<Filter> filters = new ArrayList<>();
        JSONArray filtersArray = jsonFilter.getJSONArray("filters");
        for (int i = 0; i < filtersArray.length(); i++) {
            filters.add(processJsonFilter(filtersArray.getJSONObject(i)));
        }
        filterAND.setFilters(filters);
        filter.setType(FilterFunction.AND);
        filter.setFilterAND(filterAND);
        return filter;
    }

    private static Filter processFilterOR(JSONObject jsonFilter) {
        Filter filter = new Filter();
        FilterOR filterOR = new FilterOR();
        List<Filter> filters = new ArrayList<>();
        JSONArray filtersArray = jsonFilter.getJSONArray("filters");
        for (int i = 0; i < filtersArray.length(); i++) {
            filters.add(processJsonFilter(filtersArray.getJSONObject(i)));
        }
        filterOR.setFilters(filters);
        filter.setType(FilterFunction.OR);
        filter.setFilterOR(filterOR);
        return filter;
    }

    private static Integer parseLimit(JSONObject json) {
        if (json.has("limit")) {
            return json.getInt("limit");
        }
        return null;
    }

    private static Integer parseOffset(JSONObject json) {
        if (json.has("offset")) {
            return json.getInt("offset");
        }
        return null;
    }

    private static List<String> parseFields(JSONObject json) {
        // extract fields
        JSONArray fieldsArray = json.getJSONArray("fields");
        // validate fields
        if (fieldsArray == null) {
            throw new IllegalArgumentException("field 'fields' not found");
        }
        // process fields
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < fieldsArray.length(); i++) {
            String field = fieldsArray.getString(i);
            fields.add(field);
        }
        return fields;
    }

    public static Map<String, FieldMetadata> parseMetaData(JSONObject json) {
        if (json != null) {
            return json.keySet().stream().collect(Collectors.toMap(
                    k -> k,
                    k -> parseFieldMetaData(json.getJSONObject(k))
            ));
        } else {
            return null;
        }
    }

    private static FieldMetadata parseFieldMetaData(JSONObject json) {
        FieldMetadata fieldMetadata = new FieldMetadata();
        getJSON(json, "params")
                .ifPresent(v -> {
                    Map<String, String> params = v.keySet().stream().collect(Collectors.toMap(
                            k -> k,
                            v::getString
                    ));
                    fieldMetadata.setParams(params);
                });
        getJSON(json, "fieldParams")
                .ifPresent(jsonFieldParams -> {
                    FieldParams fieldParams = new FieldParams();
                    fieldMetadata.setFieldParams(fieldParams);
                    getString(jsonFieldParams, "label")
                            .ifPresent(fieldParams::setFieldLabel);
                    getString(jsonFieldParams, "timeFieldPattern")
                            .ifPresent(fieldParams::setTimeFieldPattern);
                    getString(jsonFieldParams, "timeFieldGranularity")
                            .ifPresent(v -> fieldParams.setTimeFieldGranularity(TimeGranularity.valueOf(v)));
                    getBoolean(jsonFieldParams, "isVisible")
                            .ifPresent(fieldParams::setIsVisible);
                    getBoolean(jsonFieldParams, "isPlayable").filter(isPlayable -> isPlayable)
                            .ifPresent(v -> FieldMetaFlag.addFlags(fieldMetadata, PLAYABLE));
                    getBoolean(jsonFieldParams, "isPartition").filter(isPartition -> isPartition)
                            .ifPresent(v -> FieldMetaFlag.addFlags(fieldMetadata, PARTITION));
                    getStringList(jsonFieldParams, "flags")
                            .ifPresent(list -> {
                                FieldMetaFlag[] flags = list.stream()
                                        .map(FieldMetaFlag::valueOf)
                                        .toArray(FieldMetaFlag[]::new);
                                FieldMetaFlag.addFlags(fieldMetadata, flags);
                            });
                });
        getString(json, "type")
                .ifPresent(v -> fieldMetadata.setType(FieldType.valueOf(v)));
        getString(json, "name")
                .ifPresent(fieldMetadata::setName);
        getJSON(json, "partitionParams")
                .ifPresent(jsonPartitionParams -> {
                    PartitionParams partitionParams = new PartitionParams();
                    getString(jsonPartitionParams, "fieldName").ifPresent(partitionParams::setFieldName);
                    getString(jsonPartitionParams, "func").ifPresent(partitionParams::setFunc);
                    fieldMetadata.setPartitionParams(partitionParams);
                });
        return fieldMetadata;

    }

    private static Optional<String> getString(JSONObject json, String value) {
        return json.has(value) && !json.isNull(value) ?
                Optional.of(json.getString(value)) :
                Optional.empty();
    }

    private static Optional<Boolean> getBoolean(JSONObject json, String value) {
        return json.has(value) && !json.isNull(value) ?
                Optional.of(json.getBoolean(value)) :
                Optional.empty();
    }

    private static Optional<List<String>> getStringList(JSONObject json, String value) {
        if (json.has(value) && !json.isNull(value)) {
            JSONArray values = json.getJSONArray(value);
            return of(IntStream.range(0, values.length())
                    .mapToObj(values::getString)
                    .collect(Collectors.toList()));
        } else {
            return Optional.empty();
        }

    }

    private static Optional<JSONObject> getJSON(JSONObject json, String value) {
        return json.has(value) && !JSONObject.NULL.equals(json.get(value)) ?
                Optional.of(json.getJSONObject(value)) :
                Optional.empty();
    }

    public static <T> T retrieveAndTransformOrDefault(Map<String, String> params,
                                                      String name,
                                                      Function<String, T> converter,
                                                      T defaultValue) {
        if (MapUtils.isEmpty(params) || !params.containsKey(name)) {
            return defaultValue;
        } else {
            return converter.apply(params.get(name));
        }
    }

    public static StructuredRequest readStructuredRequest(String jsonName, CollectionInfo collectionInfo)
            throws IOException {
        JSONObject json = new JSONObject(readFileFromClassPath(jsonName));
        return StructuredUtils.parseToStructured(json, collectionInfo);
    }

    public static boolean hasPercentile(StructuredRequest request) {
        AggDataRequest aggDataRequest = request.getAggDataRequest();
        if (aggDataRequest != null) {
            List<Metric> metrics = aggDataRequest.getMetrics();
            if (metrics != null && !metrics.isEmpty()) {
                return metrics.stream().anyMatch(metric -> metric.getType() == MetricType.PERCENTILES);
            }
        }
        return false;
    }

    public static void setParam(FieldMetadata field, String param, String value) {
        Map<String, String> params = ofNullable(field.getParams())
                .orElse(new HashMap<>());
        params.put(param, value);
        field.setParams(params);
    }

    public static String getParam(FieldMetadata field, String param, String defaultValue) {
        return ofNullable(ofNullable(
                field.getParams())
                .orElse(new HashMap<>())
                .get(param))
                .orElse(defaultValue);
    }

    public static boolean isGlobalGroup(StructuredRequest request) {
        return StructuredRequestType.AGG == request.getType() && CollectionUtils.isEmpty(request.getAggDataRequest().getGroups());
    }

    public static boolean hasFlags(FieldMetadata field, FieldMetaFlag... flags) {
        if (field.getFieldParams() == null || field.getFieldParams().getFlags() == null) {
            return false;
        } else {
            List<String> requiredFlags = stream(flags).map(Enum::toString).collect(Collectors.toList());
            return field.getFieldParams().getFlags().containsAll(requiredFlags);
        }
    }

    public static boolean isFilterTextSearchOnAllDocument(Filter f) {
        return f.getType() == FilterFunction.TEXT_SEARCH
                && StringUtils.isEmpty(f.getFilterTEXT_SEARCH().getPath());
    }

    public static List<Filter> flattenFilters(List<Filter> filters) {
        return flattenFilters(filters.toArray(new Filter[filters.size()]));
    }

    public static List<Filter> flattenFilters(Filter... filters) {
        return Arrays.stream(filters).flatMap(f -> {
            switch (f.getType()) {
                case AND: {
                    return f.getFilterAND().getFilters().stream()
                            .flatMap(s -> flattenFilters(s).stream());
                }
                case OR: {
                    return f.getFilterOR().getFilters().stream()
                            .flatMap(s -> flattenFilters(s).stream());
                }
                case NOT: {
                    FilterNOT filterNOT = f.getFilterNOT();
                    return flattenFilters(filterNOT.getFilter()).stream();
                }
                default: {
                    return Stream.of(f);
                }
            }
        }).collect(Collectors.toList());
    }


    public static boolean isFilterOfTypeAndField(Filter filter, String field, FilterFunction filterType) {
        if (!filter.getType().equals(filterType)) {
            return false;
        } else {
            switch (filterType) {
                case GE:
                    return filter.getFilterGE().getPath().equals(field);
                case GT:
                    return filter.getFilterGT().getPath().equals(field);
                case LE:
                    return filter.getFilterLE().getPath().equals(field);
                case LT:
                    return filter.getFilterLT().getPath().equals(field);
                case EQ:
                    return filter.getFilterEQ().getPath().equals(field);
                case EQI:
                    return filter.getFilterEQI().getPath().equals(field);
                case IN:
                    return filter.getFilterIN().getPath().equals(field);
                case IS_NULL:
                    return filter.getFilterISNULL().getPath().equals(field);
                case CONTAINS:
                    return filter.getFilterCONTAINS().getPath().equals(field);
                case ENDS_WITH:
                    return filter.getFilterENDS_WITH().getPath().equals(field);
                case STARTS_WITH:
                    return filter.getFilterSTARTS_WITH().getPath().equals(field);
                case TEXT_SEARCH:
                    return !StringUtils.isEmpty(filter.getFilterTEXT_SEARCH().getPath())
                            && filter.getFilterTEXT_SEARCH().getPath().equals(field);
                default:
                    return false;
            }
        }
    }

    public static ConnectionParameter convertConnectionParameter(JSONObject input) {
        ParameterType type = ParameterType.valueOf(input.getString("type"));
        ConnectionParameter param = new ConnectionParameter();
        param.setName(input.getString("name"));
        param.setType(type);
        param.setIsRequired(input.getBoolean("isRequired"));
        if (input.has("description")) {
            param.setDescription(input.getString("description"));
        }
        switch (type) {
            case STRING: {
                param.setParameterString(convertStringProperties(input));
                break;
            }
            case ENUM: {
                param.setParameterEnum(convertEnumProperties(input));
                break;
            }
            case INTEGER: {
                param.setParameterInteger(convertIntProperties(input));
                break;
            }
        }
        return param;
    }

    private static ParameterInteger convertIntProperties(JSONObject input) {
        ParameterInteger intProperties = new ParameterInteger();
        if (input.has("parameterInteger")) {
            JSONObject paramsJson = input.getJSONObject("parameterInteger");
            if (paramsJson.has("min")) {
                intProperties.setMin(paramsJson.getInt("min"));
            }
            if (paramsJson.has("max")) {
                intProperties.setMax(paramsJson.getInt("max"));
            }
        }
        return intProperties;
    }

    private static ParameterEnum convertEnumProperties(JSONObject input) {
        ParameterEnum parameterEnum = new ParameterEnum();
        if (input.has("parameterEnum")) {
            JSONObject paramsJson = input.getJSONObject("parameterEnum");
            if (paramsJson.has("values")) {
                JSONArray valuesArray = paramsJson.getJSONArray("values");
                List<String> values = IntStream.range(0, valuesArray.length())
                        .mapToObj(valuesArray::getString)
                        .collect(Collectors.toList());
                parameterEnum.setValues(values);
            }
        }
        return parameterEnum;
    }

    private static ParameterString convertStringProperties(JSONObject input) {
        ParameterString stringProperties = new ParameterString();
        if (input.has("parameterString")) {
            JSONObject strPropertiesJson = input.getJSONObject("parameterString");
            if (strPropertiesJson.has("maxLength")) {
                stringProperties.setMaxLength(strPropertiesJson.getInt("maxLength"));
            }
        }
        return stringProperties;
    }

    public static String getMetricName(Metric metric) {
        switch (metric.getType()) {
            case SUM:
                return metric.getSum().getField();
            case COUNT:
                return metric.getCount().getField();
            case AVG:
                return metric.getAvg().getField();
            case MIN:
                return metric.getMin().getField();
            case MAX:
                return metric.getMax().getField();
            case PERCENTILES:
                return metric.getPercentile().getField();
            case LAST_VALUE:
                return metric.getLastValue().getField();
            case DISTINCT_COUNT:
                return metric.getDistinctCount().getField();
            default:
                throw new IllegalArgumentException("unknown metric type " + metric.getType());
        }
    }

    public static String getFilterName(Filter filter) {
        if (asList(FilterFunction.AND, FilterFunction.NOT, FilterFunction.OR)
                .contains(filter.getType())) {
            throw new IllegalArgumentException("filter name can be retrieved only for simple filters");
        }
        switch (filter.getType()) {
            case GE:
                return filter.getFilterGE().getPath();
            case GT:
                return filter.getFilterGT().getPath();
            case LE:
                return filter.getFilterLE().getPath();
            case LT:
                return filter.getFilterLT().getPath();
            case EQI:
                return filter.getFilterEQI().getPath();
            case EQ:
                return filter.getFilterEQ().getPath();
            case ENDS_WITH:
                return filter.getFilterENDS_WITH().getPath();
            case STARTS_WITH:
                return filter.getFilterSTARTS_WITH().getPath();
            case TEXT_SEARCH:
                return filter.getFilterTEXT_SEARCH().getPath();
            case IS_NULL:
                return filter.getFilterISNULL().getPath();
            case IN:
                return filter.getFilterIN().getPath();
            case CONTAINS:
                return filter.getFilterCONTAINS().getPath();
            default:
                throw new IllegalArgumentException("unknown filter type " + filter.getType());
        }
    }

    public static String getGroupName(Group group) {
        switch (group.getType()) {
            case HISTOGRAM_GROUP:
                return group.getHistogramGroup().getField();
            case TIME_GROUP:
                return group.getTimeGroup().getField();
            case ATTRIBUTE_GROUP:
                return group.getAttributeGroup().getField();
            default:
                throw new IllegalArgumentException("unknown group type " + group.getType());
        }
    }

}

