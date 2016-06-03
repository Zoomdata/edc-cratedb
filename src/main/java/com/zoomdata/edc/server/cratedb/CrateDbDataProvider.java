/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.cratedb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.edc.server.core.JdbcCommons;
import com.zoomdata.edc.server.core.ParametrizedQuery;
import com.zoomdata.edc.server.core.SQLConnectionPoolKey;
import com.zoomdata.edc.server.core.processor.AsyncProcessor;
import com.zoomdata.edc.server.core.processor.SqlQueryTask;
import com.zoomdata.edc.server.core.sql.FieldMetaFlag;
import com.zoomdata.edc.server.core.sql.SQLQueryBuilder;
import com.zoomdata.edc.server.core.sql.StructuredToSQLTransformer;
import com.zoomdata.edc.server.core.utils.StringUtils;
import com.zoomdata.gen.edc.ConnectorService;
import com.zoomdata.gen.edc.request.*;
import com.zoomdata.gen.edc.types.*;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.zoomdata.gen.edc.request.ResponseStatus.*;
import static java.util.Optional.ofNullable;

public class CrateDbDataProvider implements ConnectorService.Iface {

    // we always need logger to capture problems
    private static final Logger logger = LoggerFactory.getLogger(CrateDbDataProvider.class);

    // some constants (changing them could affect performance)
    public static final int DEFAULT_FETCH_SIZE = 1_000;
    public static final int DEFAULT_TIMEOUT = 10_000;
    private static final String DRIVER_CLASS = "io.crate.client.jdbc.CrateDriver";
    private static final String validationQuery = "select 1 from information_schema.tables limit 1";

    // QueryDSL templates is an object which knows how to build
    // mysql specific queries
    private final SQLTemplates sqlTemplates = new CrateDbTemplates();

    // connection pool per datasource
    // every datasource is diferentiated by SQLConnectionPoolKey
    // and for every datasource we manage a connection pool
    private final ConcurrentHashMap<SQLConnectionPoolKey, BasicDataSource> pools = new ConcurrentHashMap<>();

    // Async Processor is responsible for processing requests
    // in a background
    private AsyncProcessor asyncProcessor = new AsyncProcessor();

    {
        asyncProcessor.initialize("CrateDb Socket Async Processor");
    }

    @Override
    public String ping() throws TException {
        logger.info("ping: received request");
        return "pong";
    }

    @Override
    public ValidateSourceResponse validateSource(ValidateSourceRequest validateSourceRequest) throws TException {
        logger.info("validateSource: " + validateSourceRequest);
        ValidateSourceResponse response = null;
        BasicDataSource pool = pool(validateSourceRequest.getRequestInfo());
        PreparedStatement ps = null;
        try (Connection connection = pool.getConnection()) {
            ps = connection.prepareStatement(validationQuery);
            List<Integer> shouldBeOne = JdbcCommons.list(ps, rs -> rs.getInt(1));
            ResponseInfo responseInfo = new ResponseInfo(ResponseStatus.SUCCESS, "OK");
            logger.info(responseInfo.toString());
            response = new ValidateSourceResponse(responseInfo);
        } catch (Exception e) {
            logger.error(e.getMessage());
            response = new ValidateSourceResponse(
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
            e.printStackTrace();
        } finally {
            JdbcCommons.closeStatement(ps);
        }
        logger.info("validateSource: " + response);
        return response;
    }

    @Override
    public ValidateCollectionResponse validateCollection(ValidateCollectionRequest validateCollectionRequest) throws TException {
        logger.info("validateCollection: " + validateCollectionRequest);
        ValidateCollectionResponse response = null;
        BasicDataSource pool = pool(validateCollectionRequest.getRequestInfo());
        PreparedStatement ps = null;
        try (Connection connection = pool.getConnection()) {
            ParametrizedQuery query = validateCollectionQuery(validateCollectionRequest.getCollectionInfo());
            ps = connection.prepareStatement(query.getSql());
            query.applyParameters(ps);
            List<Integer> shouldBeSomething = JdbcCommons.list(ps, rs -> 1);
            ResponseInfo responseInfo = new ResponseInfo(ResponseStatus.SUCCESS, "OK");
            logger.info(responseInfo.toString());
            response = new ValidateCollectionResponse(responseInfo);
        } catch (Exception e) {
            logger.error(e.getMessage());
            response = new ValidateCollectionResponse(
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        } finally {
            JdbcCommons.closeStatement(ps);
        }
        logger.info("validateCollection: " + response);
        return response;
    }

    @Override
    public ExecuteCommandResponse executeCommand(ExecuteCommandRequest request) throws TException {
        logger.info("executeCommand: " + request);
        // this api method is used to implement some
        // very specific (zoomdata-related) calls like:
        // clear caches, recreate pools, etc.
        // currently not used at all, just do nothing

        ExecuteCommandResponse response = new ExecuteCommandResponse(
                new ResponseInfo(SUCCESS, "Ignored")
        );
        logger.info("executeCommand: " + response);
        return response;
    }

    @Override
    public ServerInfoResponse info(ServerInfoRequest request) throws TException {
        logger.info("info: " + request);
        List<String> keys = request.getKeys();
        HashMap<String, String> keyValues = new HashMap<>();
        if (keys != null) {
            if (keys.size() == 1 && keys.get(0).equals("*")) { // respond for every feature
                keyValues.putAll(CrateDbInfo.INFO_PROPERTIES);
            } else {
                for (String key : keys) {
                    keyValues.put(key, CrateDbInfo.get(key));
                }
            }
        }
        ServerInfoResponse response = new ServerInfoResponse(
                keyValues,
                new ResponseInfo(ResponseStatus.SUCCESS, "OK")
        );
        logger.info("info: " + response);
        return response;
    }

    private Connection getConnection(final RequestInfo requestInfo) throws Exception {
        BasicDataSource pool = pool(requestInfo);
        return pool.getConnection();
    }

    @Override
    public MetaSchemasResponse schemas(MetaSchemasRequest request) throws TException {
        MetaSchemasResponse response = new MetaSchemasResponse();

        Connection connection;

        try {

            connection = getConnection(request.getRequestInfo());
            DatabaseMetaData dbMetadata = connection.getMetaData();
            ResultSet rs = dbMetadata.getSchemas();
            while (rs.next()) {
                response.addToSchemas(rs.getString(1));
            }
            response.setResponseInfo(new ResponseInfo(ResponseStatus.SUCCESS, "OK"));

        } catch (Exception e) {
            response = new MetaSchemasResponse(
                    Collections.emptyList(),
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
        logger.info("schemas: " + response);
        return response;
    }

    private List<String> schemas(DatabaseMetaData dbMeta) throws SQLException {
        return JdbcCommons.list(
                dbMeta.getCatalogs(),
                rs -> rs.getString(1)
        );
    }

    private Set<String> systemSchemas() {
        return ImmutableSet.of("PERFORMANCE_SCHEMA", "INNODB", "MYSQL", "SUPPORT", "TMP", "INNODB");
    }

    private List<CollectionInfo> collections(Connection connection, MetaCollectionsRequest request)
            throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        logger.info("request schema: " + request.getSchema());
        if (request.isSetSchema()) {
            return collectionsForSchema(dbMeta, request.getSchema());
        } else {
            // merge from all schemas
            logger.info("no schema available. default to doc");
            final String schema = "doc";

            final List<CollectionInfo> collectionBatch = collectionsForSchema(dbMeta, schema);

            return collectionBatch;
        }
    }

    private List<CollectionInfo> collectionsForSchema(DatabaseMetaData dbMeta, String schema) throws SQLException {
        logger.info("collectionsForSchema =" + schema);
        List<CollectionInfo> collectionInfoList = new ArrayList<>();

        ResultSet rs = dbMeta.getTables(schema, null, null, null);
        while (rs.next()) {
            logger.info("Schema: " + rs.getObject("TABLE_SCHEM") + ", Table:" + rs.getObject("TABLE_NAME"));
            CollectionInfo ci = new CollectionInfo();
            final String returnedSchema = rs.getString("TABLE_SCHEM");
            if (schema.equals(returnedSchema) == true) {
                final String tableName = rs.getString("TABLE_NAME");
                ci.setSchema(returnedSchema);
                ci.setCollection(tableName);
                ci.setParams(Collections.emptyMap());
                collectionInfoList.add(ci);
            } else {
                logger.info("Got unwanted schema.");
            }
        }
        return collectionInfoList;
    }

    @Override
    public MetaCollectionsResponse collections(MetaCollectionsRequest request) throws TException {
        logger.info("collections: " + request);
        MetaCollectionsResponse response;
        final Set<String> schemas = systemSchemas();
        BasicDataSource pool = pool(request.getRequestInfo());
        try (Connection connection = pool.getConnection()) {
            response = new MetaCollectionsResponse(
                    collections(connection, request)
                            .stream().filter(info -> !schemas.contains(info.getSchema())).collect(Collectors.toList()),
                    new ResponseInfo(ResponseStatus.SUCCESS, "OK")
            );
        } catch (Exception e) {
            response = new MetaCollectionsResponse(
                    Collections.emptyList(),
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
        logger.info("collections: " + response);
        return response;
    }

    private List<FieldMetadata> describeFields(
            Connection connection, CollectionInfo collectionInfo) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ParametrizedQuery query = validateCollectionQuery(collectionInfo);
            final String schemaName = collectionInfo.getSchema();
            final String tableName = collectionInfo.getCollection();

            //    final String queryString = query.getSql();

            final String queryString = "select column_name, data_type, table_name from information_schema.columns where table_name='" + tableName + "'";

            logger.info("queryString=" + queryString);

            ps = connection.prepareStatement(queryString);
            query.applyParameters(ps);
            rs = ps.executeQuery();
            final List<FieldMetadata> metadata = new ArrayList<>();

            while (rs.next()) {
                final String columnName = rs.getString("column_name");
                final String columnType = rs.getString("data_type");
                FieldType fieldType = CrateDbTypesMapping.metaForType(
                        StringUtils.extractType(columnType.toUpperCase())).getThriftType();
                FieldMetadata m = new FieldMetadata();

                m.setName(columnName);
                m.setType(fieldType);
                m.putToParams("RAW_TYPE", columnType);
                //    m.putToParams("RAW_TYPE_CODE", "" + columnTypeCode);

                FieldParams fieldParams = new FieldParams();
                fieldParams.setIsVisible(true);
                m.setFieldParams(fieldParams);
                metadata.add(m);
            }

            return metadata;
        } finally {
            JdbcCommons.closeResultSet(rs);
            JdbcCommons.closeStatement(ps);
        }
    }


    @Override
    public MetaDescribeResponse describe(MetaDescribeRequest request) throws TException {
        logger.info("describe: " + request);
        MetaDescribeResponse response;
        final BasicDataSource pool = pool(request.getRequestInfo());
        try (Connection connection = pool.getConnection()) {
            final List<FieldMetadata> metadata = describeFields(connection, request.getCollectionInfo());
            detectIndexedFields(connection, request, metadata);
            response = new MetaDescribeResponse(
                    metadata,
                    new ResponseInfo(ResponseStatus.SUCCESS, "OK")
            );
        } catch (Exception e) {
            response = new MetaDescribeResponse(
                    Collections.emptyList(),
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
            e.printStackTrace();
        }
        logger.info("describe: " + response);
        return response;
    }

    private String tableName(CollectionInfo collectionInfo) {
        return collectionInfo.getSchema() + "." + collectionInfo.getCollection();
    }

    private void detectIndexedFields(
            Connection connection, MetaDescribeRequest request, List<FieldMetadata> metadata)
            throws SQLException {

        // adjust metadata to reflect indexed columns
        for (FieldMetadata m : metadata) {
            FieldMetaFlag.addFlags(m, FieldMetaFlag.PLAYABLE);

        }
    }


    @Override
    public SampleResponse sample(SampleRequest request) throws TException {
        // this method is used for databases, which doesn't have predefined schema (mongo)
        // for mysql implementation we don't that
        throw new IllegalArgumentException("Not Supported");
    }

    private String generateRequestID() {
        return UUID.randomUUID().toString();
    }

    private ParametrizedQuery createParametrizedQuery(DataReadRequest request) {
        return StructuredToSQLTransformer.transform(request, createQueryBuilder(), sqlTemplates);
    }

    private SQLQueryBuilder createQueryBuilder() {
        return new SQLQueryBuilder();
    }


    @Override
    public PrepareResponse prepare(DataReadRequest request) throws ExecuteException, TException {
        logger.info("prepare: " + request);
        PrepareResponse response;
        try {
            String requestId = generateRequestID();

            BasicDataSource pool = pool(request.getRequestInfo());
            Supplier<Connection> connectionSupplier = () -> {
                try {
                    return pool.getConnection();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            };

            ParametrizedQuery query = createParametrizedQuery(request);
            final Map<String,String> collectionParams = request.getStructured().getCollectionInfo().getParams();
            if (collectionParams.get("CUSTOM_SQL") != null) {

                final String sql = request.getStructured().getCollectionInfo().getCollection();
                query = new ParametrizedQuery(sql, null);
                logger.info("Got custom query: " + query.getSql());
            }
            final String sql = query.getSql();
            logger.info("sql query=" + sql);

            int fetchSize = ofNullable(request.getRequestInfo().getParams())
                    .map(params -> params.get("fetch_size"))
                    .map(Integer::valueOf)
                    .orElse(DEFAULT_FETCH_SIZE);

            asyncProcessor.put(requestId, new SqlQueryTask(connectionSupplier, query, fetchSize));

            response = new PrepareResponse(
                    ImmutableList.of(new RequestID(requestId).setRawQuery(query.toString())),
                    new ResponseInfo(ResponseStatus.SUCCESS, "OK")
            );
        } catch (Exception e) { // something wrong happened
            logger.error("Request preparation has failed.", e);
            throw new ExecuteException(e.getMessage());
        }
        logger.info("prepare: " + response);
        return response;
    }

    @Override
    public StatusResponse status(DataRequest request) throws TException {
        return asyncProcessor.progress(request.getRequestId())
                .map(progress -> {
                    StatusResponse response = new StatusResponse();
                    response.setProgress(progress);
                    response.setRequestId(request.getRequestId());
                    response.setStatus(progress == 100.0 ? RequestStatus.DONE : RequestStatus.PROGRESS);
                    response.setResponseInfo(new ResponseInfo(ResponseStatus.SUCCESS, "OK"));
                    return response;
                }).orElseGet(() -> {
                    StatusResponse response = new StatusResponse();
                    response.setRequestId(request.getRequestId());
                    response.setStatus(RequestStatus.MISSING);
                    response.setResponseInfo(new ResponseInfo(ResponseStatus.SUCCESS, "OK"));
                    return response;
                });
    }

    @Override
    public void cancel(DataRequest request) throws TException {
        asyncProcessor.cancel(request.getRequestId());
    }

    @Override
    public DataResponse fetch(DataRequest request) throws TException {
        logger.info("fetch: " + request);
        DataResponse response;

        String requestId = request.getRequestId();

        int timeoutMillis = ofNullable(request.getRequestInfo().getParams())
                .map(params -> params.get("timeout"))
                .map(Integer::valueOf)
                .orElse(DEFAULT_TIMEOUT);

        try {
            response = asyncProcessor.fetch(requestId, timeoutMillis);
        } catch (TimeoutException e) {
            ResponseInfo responseInfo = new ResponseInfo(ResponseStatus.TIMEOUT_ERROR, "Failed by timeout");
            response = new DataResponse(Collections.<Record>emptyList(), Collections.<ResponseMetadata>emptyList(), true, responseInfo);
        } catch (Exception e) {
            throw new ExecuteException("ExecuteException: " + e.getMessage());
        }
        if (logger.isDebugEnabled()) {
            logger.debug("fetch: " + response);
        }
        return response;
    }

    private BasicDataSource pool(RequestInfo info) {
        return pools.computeIfAbsent(
                requestKey(info),

                // configure connection pool if needed
                key -> {
                    BasicDataSource ds = new BasicDataSource();
                    ds.setDriverClassName(DRIVER_CLASS);

                    String jdbcUrl = key.getJdbcUrl();
                    // hack for mysql
                    int parameterIndex = jdbcUrl.indexOf("?");
                    if (parameterIndex == -1) { // no parameters found
                        jdbcUrl += "?zeroDateTimeBehavior=convertToNull";
                    } else { // at least one parameter
                        jdbcUrl = jdbcUrl.substring(0, parameterIndex)
                                + "?zeroDateTimeBehavior=convertToNull&"
                                + jdbcUrl.substring(parameterIndex + 1, jdbcUrl.length());
                    }


                    ds.setUrl(jdbcUrl);
                    ds.setUsername(key.getUsername());
                    ds.setPassword(key.getPassword());
                    ds.setValidationQuery(validationQuery);
                    ds.setTestOnBorrow(true);
                    ds.setAccessToUnderlyingConnectionAllowed(true);
                    ds.setMinIdle(0);
                    ds.setMaxIdle(5);
                    ds.setMaxTotal(100);
                    ds.setTimeBetweenEvictionRunsMillis(1000);
                    ds.setMinEvictableIdleTimeMillis(5000);
                    ds.setMaxWaitMillis(20000);
                    return ds;
                });
    }

    private SQLConnectionPoolKey requestKey(RequestInfo info) {
        SQLConnectionPoolKey key = new SQLConnectionPoolKey();

        key.setJdbcUrl(info.getDataSourceInfo().getParams().get("JDBC_URL"));
        key.setUsername(info.getDataSourceInfo().getParams().get("USERNAME"));
        key.setPassword(info.getDataSourceInfo().getParams().get("PASSWORD"));

        return key;
    }

    private ParametrizedQuery validateCollectionQuery(CollectionInfo collectionInfo) {
        final Map<String, FieldMetadata> fieldMetadataMap = Collections.emptyMap();
        return createQueryBuilder().init(collectionInfo.getSchema(), collectionInfo.getCollection(), fieldMetadataMap)
                .withFields(Collections.singletonList("*"))
                .withLimit(1)
                .build(sqlTemplates);
    }

}
