package com.dataparse.server.service.es.index;

import com.dataparse.server.service.schema.*;
import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.tasks.TaskState;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.Delete;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class DeleteTask extends AbstractTask<DeleteRequest> {

    @Autowired
    @JsonIgnore
    private IndexService indexService;

    @Autowired
    @JsonIgnore
    private TableRepository tableRepository;

    @Override
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(DeleteTaskState.DELETE, () -> {

            List<BulkableAction> actions = new ArrayList<>();
            for (String id : getRequest().getIds()) {
                Delete update = new Delete.Builder(id).build();
                actions.add(update);
            }
            TableBookmark bookmark = tableRepository.getTableBookmark(getRequest().getBookmarkId());
            setResult(indexService.executeBulk(bookmark.getTableSchema().getExternalId(), actions, Arrays.asList()));
        });
    }

    @Override
    public void cancel() {
        // do nothing
    }
}
