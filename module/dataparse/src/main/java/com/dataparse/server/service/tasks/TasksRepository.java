package com.dataparse.server.service.tasks;

import com.dataparse.server.service.AbstractMongoRepository;
import com.dataparse.server.service.flow.FlowExecutionTask;
import com.dataparse.server.service.tasks.utils.PendingIngestionTask;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import lombok.extern.slf4j.Slf4j;
import org.mongojack.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TasksRepository extends AbstractMongoRepository {

    private JacksonDBCollection<TaskInfo, String> taskCollection;

    @PostConstruct
    public void init() throws UnknownHostException {
        super.init();
        taskCollection = JacksonDBCollection.wrap(database.getCollection("task"), TaskInfo.class, String.class);
    }

    public void saveTaskInfo(TaskInfo taskInfo) {
        taskCollection.update(DBQuery.is("_id", taskInfo.getId()), taskInfo, true, false);
    }

    public void updateTaskInfo(String taskId, DBObject object) {
        taskCollection.update(new BasicDBObject("_id", taskId), object);
    }

    public boolean setTaskStopFlag(String taskId) {
        return taskCollection.updateById(taskId, DBUpdate.set("stopFlag", true)).getN() > 0;
    }

    public Set<String> getStoppingTasks() {
        Set<String> tasks = taskCollection.find()
                .is("finished", false)
                .is("stopFlag", true).toArray()
                .stream()
                .map(TaskInfo::getId)
                .collect(Collectors.toSet());

        Set<String> childTasks = getTasksHierarchy(tasks, DBQuery.exists("parentTask"), 0)
                .stream().map(task -> task.getId())
                .collect(Collectors.toSet());
        childTasks.addAll(tasks);
        return childTasks;
    }

    public List<TaskInfo> getTasksHierarchy(Set<String> parentTasks, DBQuery.Query query, int iteration) {
        List<TaskInfo> tasks = taskCollection.find(query)
                .in("parentTask", parentTasks)
                .toArray();
        Set<String> taskIds = tasks.stream()
                .map(TaskInfo::getId)
                .collect(Collectors.toSet());
        if (!taskIds.isEmpty() && iteration < 10) {
            tasks.addAll(getTasksHierarchy(taskIds, query, iteration + 1));
        }
        return tasks;
    }

    public TaskInfo getTaskInfo(String taskId) {
        return taskCollection.findOne(DBQuery.is("_id", taskId));
    }

    public List<TaskInfo> getTaskInfo(List<String> taskIds) {
        return taskCollection.find().in("_id", taskIds).toArray();
    }


    public List<TaskInfo> getFinishedTasksInfo(Collection<String> ids) {
        return taskCollection.find()
                .in("_id", ids)
                .is("finished", true).toArray();
    }

    public TaskInfo getPendingIngestTaskInfoByChildTask(String taskId, Long userId) {
        List<TaskInfo> taskInfo = taskCollection.find()
                .is("className", PendingIngestionTask.class.getSimpleName())
                .is("request.auth.userId", userId)
                .in("request.pendingTasks", taskId)
                .is("finished", false).toArray(1);
        return taskInfo.size() == 1 ? taskInfo.get(0) : null;
    }

    public List<String> getFinishedTasks(Collection<String> ids) {
        return Lists.transform(getFinishedTasksInfo(ids), TaskInfo::getId);
    }

    public List<TaskInfo> getTaskInfo(Set<String> classNames, Function<DBCursor<TaskInfo>, DBCursor<TaskInfo>> fn) {
        DBCursor<TaskInfo> cursor = taskCollection.find()
                .in("className", classNames);
        cursor = fn.apply(cursor);
        return cursor.toArray();
    }

    public boolean removeTask(String id) {
        return taskCollection.updateById(id, DBUpdate.set("removed", true)).getN() > 0;
    }

    public List<TaskInfo> getPendingTasksAsList(Set<String> classNames, Long userId) {
        return taskCollection.find()
                .in("className", classNames)
                .is("request.auth.userId", userId)
                .is("finished", false)
                .toArray();
    }



    public List<TaskInfo> getPendingFlowTasksAsListByDatadoc(Long userId, Long datadocId) {
        return taskCollection.find()
                .is("className", FlowExecutionTask.class.getSimpleName())
                .is("request.auth.userId", userId)
                .is("request.datadocId", datadocId)
                .is("finished", false)
                .toArray();
    }

    public List<TaskInfo> getPendingFlowTasksAsListByBookmark(Long userId, Long bookmarkId) {
        return taskCollection.find()
                .is("className", FlowExecutionTask.class.getSimpleName())
                .is("request.auth.userId", userId)
                .is("result.bookmarkId", bookmarkId)
                .is("finished", false)
                .toArray();
    }

    public List<TaskInfo> getPendingTasksByIds(Collection<String> ids, Long userId) {
        return taskCollection.find()
                .in("_id", ids)
                .is("request.auth.userId", userId)
                .is("finished", false)
                .toArray();
    }

    private List<TaskInfo> getSortedTasksHierarchy(List<TaskInfo> rootTasks, DBQuery.Query query) {
        List<TaskInfo> allTasks = new LinkedList<>();
        allTasks.addAll(rootTasks);
        List<String> taskIds = rootTasks.stream()
                .map(TaskInfo::getId)
                .collect(Collectors.toList());

        if (!taskIds.isEmpty()) {
            loadChildren(allTasks, taskIds, query, 1);
        }
        return allTasks;
    }

    private void loadChildren(List<TaskInfo> destination, Collection<String> parents, DBQuery.Query baseQuery,
                              int iteration) {
        List<String> childIds = new ArrayList<>();
        Map<String, List<TaskInfo>> children = new HashMap<>((int) (parents.size() * 1.4));
        taskCollection.find(baseQuery.in("parentTask", parents)).forEach(task -> {
            childIds.add(task.getId());
            List<TaskInfo> child = children.get(task.getParentTask());
            if (child == null) {
                child = new ArrayList<>();
                children.put(task.getParentTask(), child);
            }
            task.setLevel(iteration);
            child.add(task);
        });
        Map<Integer, String> positions = new HashMap<>((int) (children.size() * 1.4));
        children.keySet().forEach(taskId -> positions.put(getIndexOrThrow(destination, taskId), taskId));
        positions
                .entrySet()
                .stream()
                .sorted((a, b) -> b.getKey() - a.getKey())
                .forEach(item -> destination.addAll(item.getKey() + 1, children.get(item.getValue())));
        if (childIds.size() > 0 && iteration < 2) {
            loadChildren(destination, childIds, baseQuery, iteration + 1);
        }
    }

    private int getIndexOrThrow(List<TaskInfo> target, String taskId) {
        int index = 0;
        Iterator<TaskInfo> iterator = target.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getId().equals(taskId)) {
                return index;
            }
            index++;
        }
        throw new RuntimeException("Task not found");
    }

    public Page<TaskInfo> getFinishedTasksHierarchy(Set<String> rootClassName, PageRequest request)//, String userId)
    {
        DBQuery.Query query = DBQuery.is("finished", true);
        DBObject sObj = getSort(request);
        List<TaskInfo> rootTasks = taskCollection.find(query)
                .in("className", rootClassName)
                //                .is("userId", userId)
                .sort(sObj)
                .skip(request.getOffset())
                .limit(request.getPageSize())
                .toArray();
        int count = taskCollection.find(query)
                .in("className", rootClassName)
                //                .is("userId", userId)
                .count();
        return new PageImpl<>(getSortedTasksHierarchy(rootTasks, query), request, count);
    }

    public String getLastPendingTask(String routingKey) {
        List<TaskInfo> result = taskCollection.find().is("routingKey", routingKey)
                .is("finished", false).toArray();
        return result.isEmpty() ? null : result.get(0).getId();
    }

    public void dropNonFinishedTasks() {
        WriteResult result = taskCollection.update(DBQuery.is("finished", false),
                                                   DBUpdate.set("finished", true), false, true);
        log.warn("Stopped {} unfinished tasks on startup.", result.getN());
    }

    public void setTaskStateFailed(String taskId) {
        taskCollection.updateById(taskId, DBUpdate.set("finished", true));
    }
}
