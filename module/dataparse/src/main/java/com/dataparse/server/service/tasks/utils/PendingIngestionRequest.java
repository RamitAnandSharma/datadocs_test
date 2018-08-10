package com.dataparse.server.service.tasks.utils;

import com.dataparse.server.service.tasks.AbstractRequest;
import com.dataparse.server.service.tasks.AbstractTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PendingIngestionRequest extends AbstractRequest {
    private Collection<String> pendingTasks;
    private Long preProcessionTime = 0L;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return PendingIngestionTask.class;
    }
}
