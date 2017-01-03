/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.api;

import com.google.common.collect.ImmutableList;
import com.zoomdata.connector.example.common.utils.StructuredUtils;
import com.zoomdata.connector.example.framework.async.AsyncProcessor;
import com.zoomdata.connector.example.framework.async.IComputeTaskFactory;
import com.zoomdata.connector.example.framework.provider.serverdescription.GenericDescriptionProvider;
import com.zoomdata.gen.edc.request.DataReadRequest;
import com.zoomdata.gen.edc.request.DataRequest;
import com.zoomdata.gen.edc.request.DataResponse;
import com.zoomdata.gen.edc.request.ExecuteException;
import com.zoomdata.gen.edc.request.PrepareResponse;
import com.zoomdata.gen.edc.request.RequestID;
import com.zoomdata.gen.edc.request.RequestInfo;
import com.zoomdata.gen.edc.request.RequestStatus;
import com.zoomdata.gen.edc.request.ResponseInfo;
import com.zoomdata.gen.edc.request.ResponseStatus;
import com.zoomdata.gen.edc.request.StatusResponse;
import com.zoomdata.gen.edc.request.serverdescription.ServerDescription;
import org.springframework.beans.factory.BeanNameAware;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.util.Optional.ofNullable;

public abstract class AbstractDataProvider implements IDataProvider, BeanNameAware {

    public static final int DEFAULT_TIMEOUT = 10_000;
    public static final int DEFAULT_FETCH_SIZE = 1_000;
    public static final String TIMEOUT_REQUEST_PARAMETER = "timeout";
    public static final String FETCH_SIZE_REQUEST_PARAMETER = "fetch_size";

    private AsyncProcessor asyncProcessor = new AsyncProcessor();
    protected String dataProviderBeanName;
    protected IDescriptionProvider descriptionProvider;

    @PostConstruct
    public void postConstruct() {
        asyncProcessor.initialize(this.getClass().getSimpleName());
        descriptionProvider = createDescriptionProvider();
    }

    @PreDestroy
    public void preDestroy() {
        asyncProcessor.shutdown();
    }

    @Override
    public PrepareResponse prepare(DataReadRequest request) throws ExecuteException {
        int fetchSize = ofNullable(request.getRequestInfo().getParams())
                .map(params -> params.get(FETCH_SIZE_REQUEST_PARAMETER))
                .map(Integer::valueOf)
                .orElse(DEFAULT_FETCH_SIZE);

        IComputeTaskFactory computeTaskFactory = createComputeTaskFactory(request, fetchSize);
        String rawQuery = computeTaskFactory.getRawQuery();
        String requestId = UUID.randomUUID().toString();

        asyncProcessor.put(requestId, computeTaskFactory);

        return new PrepareResponse(
                ImmutableList.of(new RequestID(requestId).setRawQuery(rawQuery)),
                new ResponseInfo(ResponseStatus.SUCCESS, "OK"));
    }

    @Override
    public StatusResponse status(DataRequest request) {
        return asyncProcessor.progress(request.getRequestId())
                .map(progress -> {
                    StatusResponse response = new StatusResponse();
                    response.setProgress(progress);
                    response.setRequestId(request.getRequestId());
                    response.setStatus(progress >= 100.0 ? RequestStatus.DONE : RequestStatus.PROGRESS);
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
    public void cancel(DataRequest request) {
        asyncProcessor.cancel(request.getRequestId());
    }

    @Override
    public DataResponse fetch(DataRequest request) throws ExecuteException {
        String requestId = request.getRequestId();
        int timeoutMillis = StructuredUtils.retrieveAndTransformOrDefault(
                request.getRequestInfo().getParams(),
                TIMEOUT_REQUEST_PARAMETER,
                Integer::valueOf,
                DEFAULT_TIMEOUT);

        try {
            return asyncProcessor.fetch(requestId, timeoutMillis);
        } catch (TimeoutException e) {
            ResponseInfo responseInfo = new ResponseInfo(ResponseStatus.TIMEOUT_ERROR, "Failed by timeout");
            return new DataResponse(Collections.emptyList(), Collections.emptyList(), true, responseInfo);
        } catch (Exception e) {
            throw new ExecuteException(e.getMessage());
        }
    }

    @Override
    public ServerDescription describeServer() {
        return descriptionProvider.describe();
    }

    @Override
    public void validateRequestInfo(RequestInfo requestInfo) {
        descriptionProvider.validate(requestInfo);
    }

    @Override
    public void setBeanName(String name) {
        this.dataProviderBeanName = name;
    }

    protected abstract IComputeTaskFactory createComputeTaskFactory(DataReadRequest request, int fetchSize) throws ExecuteException;

    protected IDescriptionProvider createDescriptionProvider() {
        return new GenericDescriptionProvider(dataProviderBeanName);
    }
}
