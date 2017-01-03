/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.service;

import com.zoomdata.connector.example.framework.api.IDataProvider;
import com.zoomdata.connector.example.framework.api.IFeatures;
import com.zoomdata.connector.example.framework.common.PropertiesExtractor;
import com.zoomdata.connector.example.framework.provider.ServerFeatures;
import com.zoomdata.gen.edc.ConnectorService;
import com.zoomdata.gen.edc.request.*;
import com.zoomdata.gen.edc.request.serverdescription.ServerDescription;
import com.zoomdata.gen.edc.types.SampleRecord;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.zoomdata.connector.example.common.utils.metadatabuilders.ResponseInfoBuilder.serverError;
import static java.util.Optional.ofNullable;

@Service
public class ZoomdataConnectorService implements ConnectorService.Iface {

    @SuppressWarnings("checkstyle:constantname")
    private static final Logger log = LoggerFactory.getLogger(ZoomdataConnectorService.class);

    @Value("${sample.log.limit:10}")
    private int sampleLogLimit;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Autowired
    private Map<String, IDataProvider> providersMap;

    private IFeatures serverFeatures = new ServerFeatures();

    public ZoomdataConnectorService() { }

    @Override
    public String ping() throws TException {
        log.debug("ping");
        return "pong";
    }

    @Override
    public ValidateSourceResponse validateSource(ValidateSourceRequest request) throws TException {
        logMessage(request::toString, () -> "Processing ValidateSourceRequest");
        try {
            ValidateSourceResponse response = provider(request.getRequestInfo()).pingSource(request);
            logMessage(response::toString, () -> "Received ValidateSourceResponse");
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ValidateSourceResponse(serverError(e.getMessage()));
        }
    }

    @Override
    public ValidateCollectionResponse validateCollection(ValidateCollectionRequest request) throws TException {
        logMessage(request::toString, () -> "Processing ValidateCollectionRequest");
        try {
            ValidateCollectionResponse response = provider(request.getRequestInfo()).pingCollection(request);
            logMessage(response::toString, () -> "Received ValidateCollectionResponse");
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ValidateCollectionResponse(serverError(e.getMessage()));
        }
    }

    @Override
    public ExecuteCommandResponse executeCommand(ExecuteCommandRequest request) throws TException {
        logMessage(request::toString, () -> "Processing ExecuteCommandRequest");
        ExecuteCommandResponse response = provider(request.getRequestInfo()).executeCommand(request);
        logMessage(response::toString, () -> "Received ExecuteCommandResponse");
        return response;
    }

    @Override
    public ServerInfoResponse info(ServerInfoRequest request) throws TException {
        logMessage(request::toString, () -> "Processing ServerInfoRequest");
        ServerInfoResponse response;
        if (request.getRequestInfo().getDataSourceInfo() != null) {
            response = provider(request.getRequestInfo()).info(request);
        } else {
            // some features could be asked without provider
            List<String> keys = request.getKeys();
            HashMap<String, String> keyValues = new HashMap<>();
            if (keys != null) {
                if (keys.size() == 1 && keys.get(0).equals("*")) { // respond for every feature
                    keyValues.putAll(serverFeatures.getAllFeatures()); // TODO abstract features
                } else {
                    for (String key : keys) {
                        keyValues.put(key, serverFeatures.getAllFeatures().getOrDefault(key, "UNKNOWN"));
                    }
                }
            }
            response = new ServerInfoResponse(
                    keyValues,
                    new ResponseInfo(ResponseStatus.SUCCESS, "OK")
            );
        }
        logMessage(response::toString,
            () -> String.format("Received ServerInfoResponse: %d entries", response.getValuesSize()));
        return response;
    }

    @Override
    public MetaSchemasResponse schemas(MetaSchemasRequest request) throws TException {
        logMessage(request::toString, () -> "Processing MetaSchemasRequest");
        try {
            MetaSchemasResponse response = provider(request.getRequestInfo()).schemas(request);
            logMessage(response::toString,
                () -> String.format("Received MetaSchemasResponse: %d schemas detected", response.getSchemasSize()));
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new MetaSchemasResponse(Collections.emptyList(), serverError(e.getMessage()));
        }
    }

    @Override
    public MetaCollectionsResponse collections(MetaCollectionsRequest request) throws TException {
        logMessage(request::toString, () -> "Processing MetaCollectionsRequest");
        try {
            MetaCollectionsResponse response = provider(request.getRequestInfo()).collections(request);
            logMessage(response::toString,
                () -> String.format("Received MetaCollectionsResponse: %d collections", response.getCollectionsSize()));
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new MetaCollectionsResponse(Collections.emptyList(), serverError(e.getMessage()));
        }
    }

    @Override
    public MetaDescribeResponse describe(MetaDescribeRequest request) throws TException {
        logMessage(request::toString, () -> "Processing MetaDescribeRequest");
        try {
            MetaDescribeResponse response = provider(request.getRequestInfo()).describe(request);
            logMessage(response::toString,
                () -> String.format("Received MetaDescribeRequest: %d fields", response.getFieldsSize()));
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new MetaDescribeResponse(Collections.emptyList(), serverError(e.getMessage()));
        }
    }

    @Override
    public SampleResponse sample(SampleRequest request) throws TException {
        logMessage(request::toString, () -> "Processing SampleRequest");
        try {
            SampleResponse response = provider(request.getRequestInfo()).sample(request);
            log.info("Received SampleResponse: {} records", response.getRecordsSize());
            logLimitedSample(response);
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new SampleResponse(new ArrayList<>(), serverError(e.getMessage()));
        }
    }

    @Override
    public PrepareResponse prepare(DataReadRequest request) throws ExecuteException {
        try {
            log.debug(request.toString());
            PrepareResponse response = provider(request.getRequestInfo()).prepare(request);
            log.debug(response.toString());
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ExecuteException(e.getMessage());
        }
    }

    @Override
    public StatusResponse status(DataRequest request) throws TException {
        log.info("Getting status for: {}", request);
        StatusResponse response = provider(request.getRequestInfo()).status(request);
        logMessage(response::toString, () -> String.format("Received StatusResponse: %s", response.getStatus()));
        return response;
    }

    @Override
    public void cancel(DataRequest request) throws TException {
        logMessage(() -> "Canceling the request: " + request.toString(),
            () -> "Canceling the request: " + request.getRequestId());
        provider(request.getRequestInfo()).cancel(request);
        logMessage(() -> String.format("Request %s has been canceled", request.getRequestId()),
            () -> "Request cancelled");
    }

    @Override
    public DataResponse fetch(DataRequest request) throws TException {
        try {
            log.debug(request.toString());
            DataResponse response = provider(request.getRequestInfo()).fetch(request);
            int recordsCount = Optional.ofNullable(response.getRecords()).map(List::size).orElse(0);
            log.debug("Return " + recordsCount + " records for the request {}. Info: {}", request.getRequestId(), response.getResponseInfo());
            return response;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ExecuteException(e.getMessage());
        }

    }

    @Override
    public DescribeServerResponse describeServer(DescribeServerRequest describeServerRequest) throws TException {
        Map<String, IDataProvider> dataProviders = ofNullable(providersMap).orElse(Collections.emptyMap());
        List<ServerDescription> serverDescriptions = dataProviders.entrySet().stream()
            .flatMap(e -> getServerDescription(e.getValue()))
            .collect(Collectors.toList());
        return new DescribeServerResponse(serverDescriptions);
    }

    private Stream<ServerDescription> getServerDescription(IDataProvider dataProvider) {
        try {
            ServerDescription description = dataProvider.describeServer();
            return Stream.of(description);
        } catch (Exception e) {
            log.info("failed to get description for data provider " + dataProvider.toString());
            return Stream.empty();
        }
    }

    private IDataProvider provider(RequestInfo requestInfo) {
        IDataProvider provider;
        if (providersMap == null || providersMap.isEmpty()) {
            throw new IllegalArgumentException("No providers defined");
        } else if (providersMap.size() == 1) {
            // just one provider take it, hopefully
            provider = providersMap.entrySet().stream()
                .findFirst().get().getValue();
        } else {
            // multiple providers detected resolve ambiguity by request type
            provider = providersMap.get(PropertiesExtractor.extractConnectorType(requestInfo));
            if (provider == null) {
                throw new IllegalArgumentException("Provider for request " + requestInfo + " not found");
            }
        }
        provider.validateRequestInfo(requestInfo);
        return provider;
    }

    private void logLimitedSample(SampleResponse sampleResponse) {
        if (log.isDebugEnabled()) {
            log.debug("Got {} records.  Printing first {} records of sample response:",
                sampleResponse.getRecordsSize(), sampleLogLimit);
            StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(sampleResponse.getRecordsIterator(), Spliterator.ORDERED), false)
                .limit(sampleLogLimit)
                .map(SampleRecord::toString)
                .forEach(log::debug);
        }
    }

    private void logMessage(Supplier<String> debugMessage, Supplier<String> infoMessage) {
        if (log.isDebugEnabled()) {
            log.debug(debugMessage.get());
        } else {
            log.info(infoMessage.get());
        }
    }
}
