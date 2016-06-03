/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.client;

import com.zoomdata.edc.server.cratedb.CrateDbInfo;
import com.zoomdata.gen.edc.ConnectorService;
import com.zoomdata.gen.edc.request.CollectionInfo;
import com.zoomdata.gen.edc.request.DataReadRequest;
import com.zoomdata.gen.edc.request.DataRequest;
import com.zoomdata.gen.edc.request.DataRequestType;
import com.zoomdata.gen.edc.request.DataResponse;
import com.zoomdata.gen.edc.request.DataSourceInfo;
import com.zoomdata.gen.edc.request.MetaCollectionsRequest;
import com.zoomdata.gen.edc.request.MetaCollectionsResponse;
import com.zoomdata.gen.edc.request.MetaDescribeRequest;
import com.zoomdata.gen.edc.request.MetaDescribeResponse;
import com.zoomdata.gen.edc.request.MetaSchemasRequest;
import com.zoomdata.gen.edc.request.MetaSchemasResponse;
import com.zoomdata.gen.edc.request.PrepareResponse;
import com.zoomdata.gen.edc.request.RawDataRequest;
import com.zoomdata.gen.edc.request.RequestInfo;
import com.zoomdata.gen.edc.request.ServerInfoRequest;
import com.zoomdata.gen.edc.request.ServerInfoResponse;
import com.zoomdata.gen.edc.request.StructuredRequest;
import com.zoomdata.gen.edc.request.StructuredRequestType;
import com.zoomdata.gen.edc.request.ValidateCollectionRequest;
import com.zoomdata.gen.edc.request.ValidateCollectionResponse;
import com.zoomdata.gen.edc.request.ValidateSourceRequest;
import com.zoomdata.gen.edc.request.ValidateSourceResponse;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {
    public static void main(String[] args) throws TException {
        int port = 7337;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        System.out.println("Client started at port " + port + " ...");

        ConnectorService.Client client = createClient(port);
        System.out.println("Client created...");

        Map<String, String> dsParams = new HashMap<>();
        dsParams.put("JDBC_URL", "crate://10.2.1.251:4300");
     //   dsParams.put("USERNAME", "");
     //   dsParams.put("PASSWORD", "");
        System.out.println("Datasource configuration: " + dsParams);

        CollectionInfo collectionInfo = new CollectionInfo();
        collectionInfo.setSchema("doc");
        collectionInfo.setCollection("tweets");
        System.out.println("Collection configuration: " + collectionInfo);


        System.out.println("= API EXAMPLES =");

        {
            System.out.println("1. Ping Server");
            System.out.println("\t test connectivity to edc server");

            String pingResponse = client.ping();

            System.out.println("Response: " + pingResponse);
        }


        {
            System.out.println("2. Validate Source");
            System.out.println("\t test connectivity to datasource through EDC");

            ValidateSourceRequest request = new ValidateSourceRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));

            ValidateSourceResponse response = client.validateSource(request);

            System.out.println("Response: " + response);
        }

        {
            System.out.println("3. Validate Collection");
            System.out.println("\t test connectivity to table [" + collectionInfo.getCollection() + "]");

            ValidateCollectionRequest request = new ValidateCollectionRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));
            request.setCollectionInfo(collectionInfo);

            ValidateCollectionResponse response = client.validateCollection(request);

            System.out.println("Response: " + response);
        }

        {
            System.out.println("4. Info Request");
            System.out.println("\t check server/datasource specific features");

            ServerInfoRequest request = new ServerInfoRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));

            List<String> keys = new ArrayList<>();
            keys.addAll(CrateDbInfo.INFO_PROPERTIES.keySet());
            request.setKeys(keys);

            ServerInfoResponse response = client.info(request);

            System.out.println("Response: " + response);
        }

        {
            System.out.println("5. List Schemas");
            System.out.println("\t list available schemas in datasource");

            MetaSchemasRequest request = new MetaSchemasRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));

            MetaSchemasResponse response = client.schemas(request);

            System.out.println("Response: " + response);
        }

        {
            System.out.println("6. List Collections");
            System.out.println("\t list available collections in schema [" + collectionInfo.getSchema() + "]");

            MetaCollectionsRequest request = new MetaCollectionsRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));

            MetaCollectionsResponse response = client.collections(request);

            System.out.println("Response: " + response);
        }

        {
            System.out.println("7. Describe Request");
            System.out.println("\t get metadata for table [" + collectionInfo.getCollection() + "]");

            MetaDescribeRequest request = new MetaDescribeRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));
            request.setCollectionInfo(collectionInfo);

            MetaDescribeResponse response = client.describe(request);

            System.out.println("Response: " + response);
        }

        {
            System.out.println("8. Raw Data Request");
            System.out.println("\t get raw data without aggregations");

            DataReadRequest request = new DataReadRequest();
            request.setRequestInfo(makeRequestInfo(dsParams));
            request.setType(DataRequestType.STRUCTURED);
            StructuredRequest structuredRequest = new StructuredRequest();
            structuredRequest.setCollectionInfo(collectionInfo);
            structuredRequest.setType(StructuredRequestType.RAW);
            RawDataRequest rawDataRequest = new RawDataRequest();
            rawDataRequest.setFields(Arrays.asList("retweeted"));
            rawDataRequest.setLimit(100);
            structuredRequest.setRawDataRequest(rawDataRequest);
            request.setStructured(structuredRequest);

            PrepareResponse response = client.prepare(request);
            System.out.println("Query Prepared:" + response.getRequestIds());

            String queryId = response.getRequestIds().get(0).getId();
            System.out.println("Fetching [by 10]...");
            int batch = 1;
            while (true) {
                DataRequest dataRequest = new DataRequest();
                dataRequest.setRequestInfo(makeRequestInfo(dsParams));
                dataRequest.setRequestId(queryId);
                DataResponse dataResponse = client.fetch(dataRequest);
                System.out.println("Data Response Batch [" + batch + "] :" + dataResponse);
                if (!dataResponse.isHasNext()) {
                    break;
                }
                batch++;
            }
            System.out.println("Fetching Done.");
        }

        System.exit(0);

        {
            System.out.println("9. Stats Data Request");
            System.out.println("\t get statistics for columns (min/max)");


        }

        {
            System.out.println("10. Distincs Values Request");
            System.out.println("\t get all distinct values for field");


        }

        {
            System.out.println("11. Aggregated Data Request");
            System.out.println("\t get aggregated data");


        }


    }

    private static RequestInfo makeRequestInfo(Map<String, String> dsParams) {
        return new RequestInfo()
                .setUsername("client")
                .setDataSourceInfo(new DataSourceInfo(dsParams));
    }

    private static ConnectorService.Client createClient(final int port) throws TTransportException {
        TTransport transport = new TSocket("localhost", port, 60 * 1000);
        TProtocol protocol = new TCompactProtocol(transport);
        ConnectorService.Client client = new ConnectorService.Client(protocol);
        transport.open();
        return client;
    }
}
