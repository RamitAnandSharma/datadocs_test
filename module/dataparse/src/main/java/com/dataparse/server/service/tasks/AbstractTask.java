package com.dataparse.server.service.tasks;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.mongodb.MongoInterruptedException;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class AbstractTask<T extends AbstractRequest> implements Cancellable
{
    public static final int READ_TIMEOUT = 5000;
    public static final int CONNECT_TIMEOUT = 10000;
    private static final String WORKER_KEY = "worker";
    private static final String REQUEST_TYPE = "request_type";

    private String id;
    private T request;
    private TaskState lastState;
    private boolean finished;
    private boolean interrupted;

    private TaskResult result;

    @Autowired
    @JsonIgnore
    private TasksRepository tasksRepository;

    public void saveResult(TaskResult result){
        this.result = result;
        saveState();
    }

    public void saveState(){
        try {
            tasksRepository.updateTaskInfo(getId(), TaskInfo.update(this));
        } catch (MongoInterruptedException e){
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void onBeforeStart(TaskInfo info){
        // empty
    }

    public void onAfterFinish(TaskInfo info){
        // empty
    }

    public void onTaskStopped(TaskInfo info){
        // empty
    }

    @JsonIgnore
    public abstract Map<TaskState, Runnable> getStates();

    public void init(T request){
        this.request = request;
    }

    public String routingKey()
    {
        return getRequest().getRoutingKey();
    }

}
