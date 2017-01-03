/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.provider;

import com.google.common.base.Supplier;
import com.querydsl.sql.SQLTemplates;
import com.zoomdata.connector.example.common.utils.FieldMetaFlag;
import com.zoomdata.connector.example.common.utils.StringUtils;
import com.zoomdata.connector.example.common.utils.ThriftUtils;
import com.zoomdata.connector.example.framework.api.AbstractDataProvider;
import com.zoomdata.connector.example.framework.api.IFeatures;
import com.zoomdata.connector.example.framework.api.ITypesMapping;
import com.zoomdata.connector.example.framework.async.IComputeTaskFactory;
import com.zoomdata.connector.example.framework.common.JdbcCommons;
import com.zoomdata.connector.example.framework.common.PropertiesExtractor;
import com.zoomdata.connector.example.framework.common.SQLConnectionPoolKey;
import com.zoomdata.connector.example.framework.common.sql.ParametrizedQuery;
import com.zoomdata.connector.example.framework.common.sql.SQLQueryBuilder;
import com.zoomdata.connector.example.framework.common.sql.StructuredToSQLTransformer;
import com.zoomdata.connector.example.framework.common.sql.impl.DefaultSQLQueryBuilder;
import com.zoomdata.gen.edc.request.*;
import com.zoomdata.gen.edc.types.FieldMetadata;
import com.zoomdata.gen.edc.types.FieldParams;
import com.zoomdata.gen.edc.types.FieldType;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.zoomdata.connector.example.common.utils.FieldMetaFlag.PLAYABLE;
import static com.zoomdata.connector.example.common.utils.StreamUtils.not;
import static com.zoomdata.connector.example.framework.common.sql.impl.Utils.isCustomSql;
import static java.util.Optional.ofNullable;

@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class GenericSQLDataProvider extends AbstractDataProvider {

    @SuppressWarnings("checkstyle:constantname")
    private static final Logger log = LoggerFactory.getLogger(GenericSQLDataProvider.class);

    public static final String DEFAULT_VALIDATE_SOURCE_QUERY = "select 1";

    @Value("${jdbc.connection.timeout.sec:60}")
    protected int jdbcConnectionTimeoutSec;

    protected SQLTemplates sqlTemplates;
    protected ITypesMapping typesMapping;

    protected IFeatures features;

    // pool per datasource
    protected final ConcurrentHashMap<SQLConnectionPoolKey, BasicDataSource> pools =
            new ConcurrentHashMap<>();
    @Value("${datasource.min.idle}")
    private int minIdle = 0;

    @Value("${datasource.max.idle}")
    private int maxIdle = 5;

    @Value("${datasource.max.active}")
    private int maxActiveConnections = 100;

    @Value("${datasource.max.idle.time.sec}")
    private int maxIdleTimeSec = 5;

    @Value("${datasource.max.wait.time.sec}")
    private int maxWaitTime = 20;

    @Value("${datasource.eviction.time.between.sec}")
    private int evictionTimeBetween = 1;

    public GenericSQLDataProvider(SQLTemplates sqlTemplates, ITypesMapping typesMapping, IFeatures features) {
        this.sqlTemplates = sqlTemplates;
        this.typesMapping = typesMapping;
        this.features = features;
    }

    /**
     * Since Hive2 JDBC driver that we use to connect to Impala doesn't support other ways to specify login timeout
     * and DriverManager.setLoginTimeout is static, I leave it here because it may affect other connectors.
     */
    @PostConstruct
    private void setUpConnectionTimeout() {
        DriverManager.setLoginTimeout(jdbcConnectionTimeoutSec);
    }

    public SQLQueryBuilder createSqlQueryBuilder() {
        return new DefaultSQLQueryBuilder();
    }

    protected ResultSet executeQuery(Connection connection, ParametrizedQuery query) throws SQLException {
        final PreparedStatement statement = connection.prepareStatement(query.getSql());
        try {
            query.applyParameters(statement);
            return statement.executeQuery();
        } catch (SQLException e) {
            JdbcCommons.closeStatement(statement);
            throw e;
        }
    }

    @Override
    public ServerInfoResponse info(ServerInfoRequest request) {
        List<String> keys = request.getKeys();
        HashMap<String, String> keyValues = new HashMap<>();
        if (keys != null) {
            if (keys.size() == 1 && keys.get(0).equals("*")) { // respond for every feature
                keyValues.putAll(features.getAllFeatures()); // TODO abstract features
            } else {
                for (String key : keys) {
                    keyValues.put(key, features.getAllFeatures().getOrDefault(key, "UNKNOWN"));
                }
            }
        }
        return new ServerInfoResponse(
                keyValues,
                new ResponseInfo(ResponseStatus.SUCCESS, "OK")
        );
    }

    @Override
    public ExecuteCommandResponse executeCommand(ExecuteCommandRequest request) {
        String command = request.getCommand();
        Map<String, String> params = request.getCommandParams();
        if (params != null) {
            for (Map.Entry<String, String> kvPair : params.entrySet()) {
                String key = kvPair.getKey();
                String value = kvPair.getValue();
                // TODO process key/value pair or
                // TODO return ResponseStatus.CLIENT_ERROR if something wrong
            }
        }
        return new ExecuteCommandResponse(new ResponseInfo(ResponseStatus.SUCCESS, "OK"));
    }

    @Override
    public ValidateSourceResponse pingSource(ValidateSourceRequest request) {
        try (Connection connection = createConnection(request.getRequestInfo())) {
            final ResultSet resultSet = executeQuery(connection, new ParametrizedQuery(validateSourceQuery()));
            final Statement resultSetStatement = resultSet.getStatement();
            try {
                List<Integer> shouldBeOne = JdbcCommons.list(resultSet, rs -> rs.getInt(1));
                ResponseInfo responseInfo = new ResponseInfo(ResponseStatus.SUCCESS, "OK");
                return new ValidateSourceResponse(responseInfo);
            } finally {
                JdbcCommons.closeResultSet(resultSet);
                JdbcCommons.closeStatement(resultSetStatement);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ValidateSourceResponse(
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
    }

    @Override
    public ValidateCollectionResponse pingCollection(ValidateCollectionRequest request) {
        try (Connection connection = createConnection(request.getRequestInfo())) {
            ParametrizedQuery query = validateCollectionQuery(
                request.getCollectionInfo(),
                ofNullable(connection.getCatalog()));
            final ResultSet resultSet = executeQuery(connection, query);
            final Statement resultSetStatement = resultSet.getStatement();
            try {
                List<Integer> shouldBeSomething = JdbcCommons.list(resultSet, rs -> 1);
                ResponseInfo responseInfo = new ResponseInfo(ResponseStatus.SUCCESS, "OK");
                return new ValidateCollectionResponse(responseInfo);
            } finally {
                JdbcCommons.closeResultSet(resultSet);
                JdbcCommons.closeStatement(resultSetStatement);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ValidateCollectionResponse(
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
    }

    /**
     * Override this to provide custom logic for retrieving schemas from connection.
     *
     * Do not close connection, it is handled later
     *
     * @param connection database connection
     * @return list of schema names
     */
    protected List<String> schemas(Connection connection, MetaSchemasRequest request)
            throws SQLException {
        // throw exception if schemas not supported like this
        //throw new IllegalArgumentException("YOU SAID SCHEMAS NOT SUPPORTED");
        DatabaseMetaData dbMeta = connection.getMetaData();
        return JdbcCommons.listAndClose(
                dbMeta.getSchemas(),
                rs -> rs.getString("TABLE_SCHEM")
        );
    }

    /**
     *
     * @return list of system schema that must not be show on zoomdata UI
     */
    protected Set<String> systemSchemas() {
        return Collections.emptySet();
    }

    @Override
    public MetaSchemasResponse schemas(MetaSchemasRequest request) {
        Set<String> systemSchemas = getSystemSchemasInLowerCase();
        try (Connection connection = createConnection(request.getRequestInfo())) {
            List<String> schemas = schemas(connection, request).stream()
                .filter(not(StringUtils::isEmpty))
                .filter(not(schema -> systemSchemas.contains(schema.toLowerCase())))
                .collect(Collectors.toList());
            return new MetaSchemasResponse(schemas, new ResponseInfo(ResponseStatus.SUCCESS, "OK"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new MetaSchemasResponse(
                    Collections.emptyList(),
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
    }

    /**
     * Override this to provide custom logic for retrieving collections from connection.
     * You can inspect addition parameters in MetaCollectionsRequest
     *
     * Do not close connection, it is handled later
     *
     * @param connection database connection
     * @param request collections request
     * @return list of collections
     */
    protected List<CollectionInfo> collections(
            Connection connection, MetaCollectionsRequest request) throws SQLException {
        DatabaseMetaData dbMeta = connection.getMetaData();
        return JdbcCommons.listAndClose(
                dbMeta.getTables(null, request.getSchema(), "%", jdbcTableTypes()),
                rs -> ThriftUtils.getCollectionInfo(rs.getString("TABLE_SCHEM"), rs.getString("TABLE_NAME"))
        );
    }

    protected String[] jdbcTableTypes() {
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    public MetaCollectionsResponse collections(MetaCollectionsRequest request) {
        Set<String> systemSchemas = getSystemSchemasInLowerCase();
        try (Connection connection = createConnection(request.getRequestInfo())) {
            List<CollectionInfo> collections = collections(connection, request).stream()
                .filter(not(info -> info.getSchema() != null && systemSchemas.contains(info.getSchema().toLowerCase())))
                .collect(Collectors.toList());
            return new MetaCollectionsResponse(collections, new ResponseInfo(ResponseStatus.SUCCESS, "OK"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new MetaCollectionsResponse(
                    Collections.emptyList(),
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
    }

    protected List<FieldMetadata> describeFields(
            Connection connection, CollectionInfo collectionInfo) throws SQLException {
        ParametrizedQuery query = validateCollectionQuery(collectionInfo, ofNullable(connection.getCatalog()));
        final ResultSet rs = executeQuery(connection, query);
        final Statement ignored = rs.getStatement();
        try {
            ResultSetMetaData resultSetMetadata = rs.getMetaData();

            List<FieldMetadata> metadata = new ArrayList<>();
            for (int i = 1; i <= resultSetMetadata.getColumnCount(); i++) {
                FieldMetadata m = new FieldMetadata();
                String columnName = resultSetMetadata.getColumnLabel(i);
                String columnType = extractColumnTypeName(resultSetMetadata, i);
                // TODO do something with column type
                int columnTypeCode = extractColumnType(resultSetMetadata, i);

                FieldType fieldType = typesMapping.metaForType(
                    StringUtils.extractType(columnType.toUpperCase())).getThriftType();

                m.setName(columnName);
                m.setType(fieldType);
                m.putToParams("RAW_TYPE", columnType.toUpperCase());
                m.putToParams("RAW_TYPE_CODE", "" + columnTypeCode);

                FieldParams fieldParams = new FieldParams();
                fieldParams.setIsVisible(true);
                m.setFieldParams(fieldParams);

                if (isRawData(columnTypeCode)) {
                    FieldMetaFlag.addFlags(m, FieldMetaFlag.RAW_DATA_ONLY);
                }

                metadata.add(m);
            }
            return metadata;
        } finally {
            JdbcCommons.closeResultSet(rs);
            JdbcCommons.closeStatement(ignored);
        }
    }

    protected boolean isRawData(int columnTypeCode) {
        return columnTypeCode == Types.LONGVARCHAR
            || columnTypeCode == Types.LONGNVARCHAR
            || columnTypeCode == Types.LONGVARBINARY
            || columnTypeCode == Types.VARBINARY
            || columnTypeCode == Types.CLOB
            || columnTypeCode == Types.NCLOB
            || columnTypeCode == Types.SQLXML
            || columnTypeCode == Types.BLOB
            || columnTypeCode == Types.BINARY
            || columnTypeCode == Types.ARRAY
            || columnTypeCode == Types.STRUCT
            || columnTypeCode == Types.OTHER;
    }

    @Override
    public MetaDescribeResponse describe(MetaDescribeRequest request) {
        try (Connection connection = createConnection(request.getRequestInfo())) {
            List<FieldMetadata> metadata = describeFields(connection, request.getCollectionInfo());
            // fill indexes.
            final Boolean customSql = isCustomSql(request.getCollectionInfo());
            if (!customSql) {
                detectIndexedFields(connection, request, metadata);
            }
            return new MetaDescribeResponse(
                    metadata,
                    new ResponseInfo(ResponseStatus.SUCCESS, "OK")
            );
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new MetaDescribeResponse(
                    Collections.emptyList(),
                    new ResponseInfo(ResponseStatus.SERVER_ERROR, e.getMessage())
            );
        }
    }

    /**
     * Set indexed for indexed fields in metadata.
     *
     * @param connection database connection
     * @param request meta describe request
     * @param metadata metadata fields will be processed
     * @throws SQLException
     */
    protected void detectIndexedFields(
            Connection connection, MetaDescribeRequest request, List<FieldMetadata> metadata)
            throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        ResultSet indexInfo = metaData.getIndexInfo(
                null,
                request.getCollectionInfo().getSchema(),
                request.getCollectionInfo().getCollection(),
                false,
                true);

        List<String> indexedColumns = JdbcCommons.listAndClose(
                indexInfo,
                rs -> rs.getString("COLUMN_NAME")
        );

        // adjust metadata to reflect indexed columns
        for (FieldMetadata m : metadata) {
            if (indexedColumns.contains(m.getName())) {
                FieldMetaFlag.addFlags(m, PLAYABLE);
            }
        }
    }

    @Override
    protected IComputeTaskFactory createComputeTaskFactory(DataReadRequest request, int fetchSize) throws ExecuteException {
        try {
            Supplier<Connection> connectionSupplier = () -> {
                try {
                    return createConnection(request.getRequestInfo());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            };

            ParametrizedQuery query = createParametrizedQuery(request);

            return newComputeTaskFactory(connectionSupplier, query, fetchSize);
        } catch (Exception e) { // something wrong happened
            log.error("Request preparation has failed.", e);
            throw new ExecuteException(e.getMessage());
        }
    }

    protected ParametrizedQuery createParametrizedQuery(DataReadRequest request) {
        return StructuredToSQLTransformer.transform(request, createSqlQueryBuilder(), sqlTemplates);
    }

    protected IComputeTaskFactory newComputeTaskFactory(Supplier<Connection> connectionSupplier, ParametrizedQuery query, int fetchSize) {
        return new SqlPreparedQueryComputeTaskFactory(connectionSupplier, query, typesMapping, fetchSize);
    }

    /**
     * Generate key for pool by content of request info.
     *
     * @param info request info
     * @return immutable key
     */
    protected SQLConnectionPoolKey keyFromRequestInfo(RequestInfo info) {
        return PropertiesExtractor.poolKey(info);
    }

    protected abstract String jdbcClassName();

    protected BasicDataSource setupConnectionPool(SQLConnectionPoolKey key) {
        log.info("New pool with key " + key);
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(jdbcClassName());
        ds.setUrl(key.getJdbcUrl());
        ds.setUsername(key.getUsername());
        ds.setPassword(key.getPassword());
        ds.setValidationQuery(validateSourceQuery());
        ds.setTestOnBorrow(true);
        ds.setAccessToUnderlyingConnectionAllowed(true);
        ds.setMinIdle(minIdle);
        ds.setMaxIdle(maxIdle);
        ds.setMaxTotal(maxActiveConnections);
        ds.setMaxWaitMillis(TimeUnit.SECONDS.toMillis(maxWaitTime));
        ds.setMinEvictableIdleTimeMillis(TimeUnit.SECONDS.toMillis(maxIdleTimeSec));
        ds.setTimeBetweenEvictionRunsMillis(TimeUnit.SECONDS.toMillis(evictionTimeBetween));
        return ds;
    }

    protected String validateSourceQuery() {
        return DEFAULT_VALIDATE_SOURCE_QUERY;
    }

    protected ParametrizedQuery validateCollectionQuery(CollectionInfo collectionInfo, Optional<String> catalog) {
        final Boolean customSql = isCustomSql(collectionInfo);
        final Map<String, FieldMetadata> fieldMetadataMap = Collections.emptyMap();
        return createSqlQueryBuilder().init(collectionInfo.getSchema(), collectionInfo.getCollection(), customSql, fieldMetadataMap)
                .withFields(Collections.singletonList("*"))
                .withLimit(1)
                .build(sqlTemplates);
    }

    protected BasicDataSource pool(RequestInfo info) {
        return pools.computeIfAbsent(
            keyFromRequestInfo(info),
            this::setupConnectionPool);
    }

    protected Connection createConnection(RequestInfo requestInfo) throws SQLException {
        return pool(requestInfo).getConnection();
    }

    protected Set<String> getSystemSchemasInLowerCase() {
        return systemSchemas().stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    }

    protected String extractColumnTypeName(ResultSetMetaData resultSetMetaData, int index) throws SQLException {
        return JdbcCommons.extractColumnTypeName(resultSetMetaData, index);
    }

    protected int extractColumnType(ResultSetMetaData resultSetMetaData, int index) throws SQLException {
        return resultSetMetaData.getColumnType(index);
    }
}
