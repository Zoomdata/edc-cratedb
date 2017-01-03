/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb;

import com.google.common.collect.ImmutableSet;
import com.zoomdata.connector.example.framework.annotation.Connector;
import com.zoomdata.connector.example.framework.api.IDescriptionProvider;
import com.zoomdata.connector.example.framework.common.sql.SQLQueryBuilder;
import com.zoomdata.connector.example.framework.provider.GenericSQLDataProvider;
import com.zoomdata.connector.example.framework.provider.serverdescription.GenericDescriptionProvider;
import com.zoomdata.connector.example.provider.cratedb.sql.CrateDBSQLQueryBuilder;
import com.zoomdata.gen.edc.request.MetaDescribeRequest;
import com.zoomdata.gen.edc.types.FieldMetadata;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl.PasswordConnectionParameter.PasswordConnectionParameterBuilder.passwordParameter;
import static com.zoomdata.connector.example.framework.provider.serverdescription.connectionparameters.impl.StringConnectionParameter.StringConnectionParameterBuilder.stringParameter;
import static com.zoomdata.connector.example.provider.cratedb.CrateDBDataProvider.CONNECTION_TYPE;

@Connector(CONNECTION_TYPE)
public class CrateDBDataProvider extends GenericSQLDataProvider {

    // The unique connection type that will be registered in Zoomdata
    protected final static String CONNECTION_TYPE = "CRATEDB";

    // This is how we'll assign special flats for Zoomdata such as PARTITION
    private final CrateDBMetaFlagsDetector metaFlagsDetector;

    public CrateDBDataProvider() {
        super(
            new CrateDBSQLTemplates(),
            new CrateDBTypesMapping(),
            new CrateDBFeatures()
        );
        metaFlagsDetector = new CrateDBMetaFlagsDetector(sqlTemplates);
    }

    @Override
    public SQLQueryBuilder createSqlQueryBuilder() {
        return new CrateDBSQLQueryBuilder();
    }

    @Override
    protected Set<String> systemSchemas() {
        // A list of schemas used by the data source that we shouldn't display to end users
        return ImmutableSet.of("blob", "information_schema", "pg_catalog", "sys");
    }

    @Override
    protected String jdbcClassName() {
        return "io.crate.client.jdbc.CrateDriver";
    }

    @Override
    protected String validateSourceQuery() {
        return "select 1 from information_schema.tables limit 1";
    }

    @Override
    protected int extractColumnType(ResultSetMetaData metaData, int index) {
        // CrateDB's JDBC driver will throw SQLException for geo types like
        // type 'geo_point' not supported by JDBC driver so we'll just return an arbitrary code
        try {
            return metaData.getColumnType(index);
        } catch (SQLException e) {
            return -1;
        }
    }

    // This is for assigning PLAYABLE fields in Zoomdata, which it generally considers "fast" fields that can be filtered
    // quickly. For traditional RDBMS databases, this often means indexed fields. In CrateDB's case, we're looking for
    // fields that are partitions
    @Override
    protected void detectIndexedFields(Connection connection, MetaDescribeRequest request, List<FieldMetadata> metadata) throws SQLException {
        metaFlagsDetector.populateMetaFlags(connection, request.getCollectionInfo(), metadata);
    }

    @Override
    protected IDescriptionProvider createDescriptionProvider() {
        return new GenericDescriptionProvider(CONNECTION_TYPE)
            .addParameters(stringParameter("JDBC_URL").isRequired(true).description("Specify JDBC URL in the required format"))
            .addParameters(stringParameter("USER_NAME").description("Specify the user name if connecting to your database requires authentication"))
            .addParameters(passwordParameter("PASSWORD").description("Specify the password if connecting to your database requires authentication"))
            .maxVersion("0.5")
            .minVersion("0.5")
            .svgIcon("/crateio_logo.svg");
    }

}

