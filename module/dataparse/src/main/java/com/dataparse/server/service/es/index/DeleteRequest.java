package com.dataparse.server.service.es.index;

import com.dataparse.server.service.tasks.AbstractRequest;
import com.dataparse.server.service.tasks.AbstractTask;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeleteRequest extends AbstractRequest {

    private Long bookmarkId;
    private List<String> ids;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return DeleteTask.class;
    }
}
