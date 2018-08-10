package com.dataparse.server.service.tasks;

import com.dataparse.server.util.JsonUtils;
import com.fasterxml.jackson.annotation.*;
import com.mongodb.DBObject;
import lombok.Data;
import org.mongojack.DBUpdate;
import org.mongojack.Id;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskInfo {

    @Id
    private String id;
    private String parentTask;
    private String state;
    private String className;
    private String routingKey;
    private int level;
    private TaskStatistics statistics = new TaskStatistics();

    private boolean stopFlag;
    private boolean preview;
    private boolean finished;
    private boolean removed;

    private AbstractRequest request;
    private TaskResult result;

    @JsonIgnore
    public boolean isError(){
        return statistics.getErrors() != null || statistics.getLastErrorMessage() != null;
    }

    private static DBUpdate.Builder createUpdateBuilder(AbstractTask task){
        DBUpdate.Builder result = DBUpdate
                .set("finished", task.isFinished())
                .set("className", task.getClass().getSimpleName())
                .set("routingKey", task.routingKey());

        if(task.getLastState() != null){
            result.set("state", task.getLastState().toString());
        }
        if(task.getRequest().getParentTask() != null) {
            result.set("parentTask", task.getRequest().getParentTask());
        }
        result.set("result", task.getResult());
        return result;
    }

    public static DBObject update(AbstractTask task, TaskInfo taskInfo)
    {
        DBUpdate.Builder result = createUpdateBuilder(task);
        result.set("statistics", taskInfo.getStatistics());
        return result.serialiseAndGet(JsonUtils.mapper, JsonUtils.mapper.constructType(TaskResult.class));
    }

    public static DBObject update(AbstractTask task)
    {
        // custom query builder is used instead of object
        // because stopFlag value should not be updated!
        DBUpdate.Builder result = createUpdateBuilder(task);
        return result.serialiseAndGet(JsonUtils.mapper, JsonUtils.mapper.constructType(TaskResult.class));
    }

    public static TaskInfo create(AbstractTask task) {
        TaskInfo info = new TaskInfo();
        info.setId(task.getId());
        info.setRequest(task.getRequest());
        info.setFinished(task.isFinished());
        info.setClassName(task.getClass().getSimpleName());
        info.setRoutingKey(task.routingKey());
        info.setResult(task.getResult());
        if(task.getLastState() != null) {
            info.setState(task.getLastState().toString());
        }
        info.setParentTask(task.getRequest().getParentTask());
        return info;
    }
}
