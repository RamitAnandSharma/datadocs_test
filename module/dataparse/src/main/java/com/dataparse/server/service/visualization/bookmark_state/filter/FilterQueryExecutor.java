package com.dataparse.server.service.visualization.bookmark_state.filter;

import com.dataparse.server.util.stripedexecutor.StripedExecutorService;
import com.dataparse.server.util.stripedexecutor.StripedRunnable;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class FilterQueryExecutor {

    private StripedExecutorService executor;

    private final Map<Long, Map<String, StripedRunnable>> stripedDelayedExecution = new ConcurrentHashMap<>(300);

    @PostConstruct
    protected void init() {
        this.executor = new StripedExecutorService(50);
    }

    public void run(StripedRunnable runnable) {
        executor.submit(runnable);
    }

    public void addToQueue(Long id, String key, StripedRunnable task) {
        Map<String, StripedRunnable> tasksMap = stripedDelayedExecution.getOrDefault(id, new ConcurrentHashMap<>());
        tasksMap.put(key, task);
        stripedDelayedExecution.put(id, tasksMap);
    }

    public void removeOneFromExecution(String key, String fieldName) {
        executor.removeFromQueue(key + fieldName);
    }

    public void removeFromWaitQueue(Long id) {
        this.stripedDelayedExecution.remove(id);
    }

    public void removeFromWaitQueue(Long id, String field) {
        Map<String, StripedRunnable> tasks = this.stripedDelayedExecution.get(id);
        if(tasks != null) {
            tasks.remove(field);
        }
    }

    public Integer runQueue(Long key) {
        Map<String, StripedRunnable> tasks = stripedDelayedExecution.remove(key);
        if(tasks == null) {
            return 0;
        }
        Map<String, StripedRunnable> currentTasks = new HashMap<>(tasks);
        currentTasks.values().forEach(this::run);
        return currentTasks.values().size();
    }

}
