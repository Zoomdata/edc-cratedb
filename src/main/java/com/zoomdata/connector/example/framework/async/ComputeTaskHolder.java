/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

import com.zoomdata.gen.edc.request.DataResponse;
import com.zoomdata.gen.edc.request.ResponseInfo;
import com.zoomdata.gen.edc.request.ResponseStatus;
import com.zoomdata.gen.edc.types.Record;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Optional.ofNullable;

public class ComputeTaskHolder {

    private static final long LOST_TIME = 60_000L;

    @SuppressWarnings("checkstyle:constantname")
    private static final Logger log = LoggerFactory.getLogger(ComputeTaskHolder.class);

    private final String queryId;
    private final ExecutorService executor;
    private final IComputeTaskFactory taskFactory;
    private final ReentrantLock fetchLock = new ReentrantLock();
    private final ReentrantLock globalLock = new ReentrantLock();

    private volatile IComputeTask computeTask;
    private CompletableFuture<Cursor> computeFeature;
    private CompletableFuture<DataResponse> dataResponseFeature;
    private long lastTouchTime;
    private State state;

    public ComputeTaskHolder(String queryId, ExecutorService executor, IComputeTaskFactory taskFactory) {
        this.queryId = queryId;
        this.executor = executor;
        this.taskFactory = taskFactory;
        this.lastTouchTime = System.currentTimeMillis();
        this.state = State.PREPARED;
    }

    public DataResponse fetch(long timeoutMillis) throws TimeoutException {
        if (!fetchLock.tryLock()) {
            throw new AsyncException("Query " + queryId + " is being fetched");
        }

        try {
            updateState();

            if (dataResponseFeature == null) {
                dataResponseFeature = computeFeature.thenApplyAsync(this::toDataResponse, executor);
            }

            DataResponse response = dataResponseFeature.get(timeoutMillis, TimeUnit.MILLISECONDS);
            dataResponseFeature = null;
            if (!response.hasNext) {
                close(false);
            }

            return response;
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Exception during fetch query " + queryId, e);
            close(true);
            throw new AsyncException(ofNullable(ExceptionUtils.getRootCause(e)).orElse(e));
        } finally {
            updateLastTouchTime();
            fetchLock.unlock();
        }
    }

    private DataResponse toDataResponse(Cursor cursor) {
        DataResponse response = new DataResponse();
        response.setMetadata(cursor.getMetadata());
        List<Record> records = new ArrayList<>();
        while (cursor.hasNext() && records.size() < taskFactory.getFetchSize()) {
            records.add(cursor.next());
        }
        response.setRecords(records);
        response.setHasNext(cursor.hasNext() || cursor.hasNextBatch());
        response.setResponseInfo(new ResponseInfo(ResponseStatus.SUCCESS, "OK"));

        if (!cursor.hasNext() && cursor.hasNextBatch()) {
            computeFeature = CompletableFuture.supplyAsync(computeTask::compute, executor);
        }

        return response;
    }

    private void updateLastTouchTime() {
        try {
            globalLock.lock();
            lastTouchTime = System.currentTimeMillis();
        } finally {
            globalLock.unlock();
        }
    }

    private void updateState() {
        try {
            globalLock.lock();
            if (state == State.CLOSED) {
                throw new AsyncException("Query " + queryId + " is closed");
            }

            if (state == State.PREPARED) {
                log.debug("Execute query " + queryId);
                computeTask = taskFactory.create();
                computeFeature = CompletableFuture.supplyAsync(computeTask::compute, executor);
                state = State.EXECUTED;
            }

        } finally {
            globalLock.unlock();
        }
    }

    public void close(boolean cancel) {
        try {
            globalLock.lock();
            if (state == State.EXECUTED) {
                if (cancel) {
                    log.info("Cancel query " + queryId);
                    computeTask.cancel();
                }

                log.debug("Close query " + queryId);
                computeTask.close();
            }

            state = State.CLOSED;
        } finally {
            globalLock.unlock();
        }
    }

    public double progress() {
        try {
            globalLock.lock();
            if (state == State.EXECUTED) {
                return computeTask.progress();
            }

            return 0;
        } finally {
            globalLock.unlock();
        }
    }

    public boolean checkLostAndClose() {
        if (globalLock.tryLock()) {
            try {
                if (!fetchLock.isLocked() && System.currentTimeMillis() - lastTouchTime > LOST_TIME) {
                    close(true);
                    return true;
                }
            } finally {
                globalLock.unlock();
            }
        }

        return false;
    }

    private enum State {
        PREPARED, EXECUTED, CLOSED
    }

}
