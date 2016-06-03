/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.edc.server.core.processor;

import com.zoomdata.edc.server.core.Cursor;
import com.zoomdata.gen.edc.request.DataResponse;
import com.zoomdata.gen.edc.request.ResponseInfo;
import com.zoomdata.gen.edc.request.ResponseStatus;
import com.zoomdata.gen.edc.types.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Optional.ofNullable;

public class QueryTaskHolder {

    private static final long LOST_TIME = 60_000L;

    @SuppressWarnings("checkstyle:constantname")
    private static final Logger log = LoggerFactory.getLogger(QueryTaskHolder.class);

    private final String queryId;
    private final ExecutorService executor;
    private final SqlQueryTask queryTask;
    private final ReentrantLock fetchLock = new ReentrantLock();
    private final ReentrantLock globalLock = new ReentrantLock();

    private SqlComputeTask computeTask;
    private CompletableFuture<Cursor> computeFeature;
    private CompletableFuture<DataResponse> dataResponseFeature;
    private long lastTouchTime;
    private State state;

    public QueryTaskHolder(String queryId, ExecutorService executor, SqlQueryTask queryTask) {
        this.queryId = queryId;
        this.executor = executor;
        this.queryTask = queryTask;
        this.lastTouchTime = System.currentTimeMillis();
        this.state = State.PREPARED;
    }

    public DataResponse fetch(long timeoutMillis) throws TimeoutException {
        if (!fetchLock.tryLock()) {
            throw new RuntimeException("Query " + queryId + " is being fetched");
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
            close(true);
            throw new RuntimeException(ofNullable(e.getCause()).orElse(e));
        } finally {
            updateLastTouchTime();
            fetchLock.unlock();
        }
    }

    private DataResponse toDataResponse(Cursor cursor) {
        DataResponse response = new DataResponse();
        response.setMetadata(cursor.getMetadata());
        List<Record> records = new ArrayList<>();
        while (cursor.hasNext() && records.size() < queryTask.getFetchSize()) {
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
                throw new RuntimeException("Query " + queryId + " is closed");
            }

            if (state == State.PREPARED) {
                log.info("Execute query " + queryId);
                computeTask = queryTask.execute();
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

                log.info("Close query " + queryId);
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