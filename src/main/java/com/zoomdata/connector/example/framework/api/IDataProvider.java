/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.api;

import com.zoomdata.gen.edc.request.*;
import com.zoomdata.gen.edc.request.serverdescription.ServerDescription;

/**
 * DataSource related methods (connector)
 *
 * Provide implementation of this interface for specific datasource.
 * Implementation allows to use connector framework based connectivity to new datasource from Zoomdata.
 *
 */
public interface IDataProvider {

    /**
     * Test connection to datasource.
     *
     * @param validateSourceRequest request
     * @return validateSourceResponse
     */
    ValidateSourceResponse pingSource(ValidateSourceRequest validateSourceRequest);

    /**
     * Test connection to collection.
     *
     * @param validateCollectionRequest request
     * @return validateCollectionResponse
     */
    ValidateCollectionResponse pingCollection(ValidateCollectionRequest validateCollectionRequest);

    /**
     * Request properties of edc server and datasource.
     *
     * @param request request
     * @return response
     */
    ServerInfoResponse info(ServerInfoRequest request);
    ExecuteCommandResponse executeCommand(ExecuteCommandRequest request);

    /* Metadata */

    MetaSchemasResponse     schemas(MetaSchemasRequest request);
    MetaCollectionsResponse collections(MetaCollectionsRequest request);
    MetaDescribeResponse    describe(MetaDescribeRequest request);

    default SampleResponse sample(SampleRequest request) {
        throw new RuntimeException("Sample is not supported");
    }

    /* Data Requests */

    PrepareResponse prepare(DataReadRequest request) throws ExecuteException;

    /* Managing Data Request */

    StatusResponse status(DataRequest request);
    void cancel(DataRequest request);
    DataResponse fetch(DataRequest request) throws ExecuteException;

    ServerDescription describeServer();

    void validateRequestInfo(RequestInfo requestInfo);

}

