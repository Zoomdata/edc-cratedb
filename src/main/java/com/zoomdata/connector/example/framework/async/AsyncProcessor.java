/**
 * Copyright (C) Zoomdata, Inc. 2012-2017. All rights reserved.
 */
package com.zoomdata.connector.example.framework.async;

import com.zoomdata.gen.edc.request.DataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static java.util.stream.Collectors.toList;

public class AsyncProcessor {

    @SuppressWarnings("checkstyle:constantname")
    private static final Logger log = LoggerFactory.getLogger(AsyncProcessor.class);

    private ExecutorService tasksExecutor;
    private ScheduledExecutorService cleanScheduleTaskExecutor;

    private ConcurrentHashMap<String, ComputeTaskHolder> tasks = new ConcurrentHashMap<>();

    public void initialize(String simpleName) {
        tasksExecutor = Executors.newCachedThreadPool();

        cleanScheduleTaskExecutor = Executors.newSingleThreadScheduledExecutor();
        cleanScheduleTaskExecutor.scheduleAtFixedRate(() -> {
            log.debug("Tasks count by provider " + simpleName + ": " + tasks.size());

            tasks.entrySet().stream()
                    .filter(e -> e.getValue().checkLostAndClose())
                    .map(Map.Entry::getKey)
                    .collect(toList())
                    .stream()
                    .forEach(tasks::remove);
        }, 10, 10, TimeUnit.SECONDS);
    }

    public void shutdown() {
        tasksExecutor.shutdown();
        cleanScheduleTaskExecutor.shutdown();
    }

    public void put(String id, IComputeTaskFactory taskFactory) {
        if (tasks.putIfAbsent(id, new ComputeTaskHolder(id, tasksExecutor, taskFactory)) != null) {
            throw new AsyncException("Query with id " + id + " exist");
        }
    }

    public DataResponse fetch(String id, long timeout) throws TimeoutException {
        ComputeTaskHolder task = tasks.get(id);
        if (task == null) {
            throw new AsyncException("Query " + id + " not found");
        }

        DataResponse fetch = task.fetch(timeout);
        if (!fetch.isHasNext()) {
            tasks.remove(id);
        }
        return fetch;
    }

    public Optional<Double> progress(String id) {
        ComputeTaskHolder task = tasks.get(id);
        if (task == null) {
            return Optional.empty();
        }

        return Optional.of(task.progress());
    }

    public void cancel(String id) {
        ComputeTaskHolder task = tasks.remove(id);
        if (task != null) {
            task.close(true);
        }
    }
}
