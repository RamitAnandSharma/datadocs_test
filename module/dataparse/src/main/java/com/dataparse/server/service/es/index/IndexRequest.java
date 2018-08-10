package com.dataparse.server.service.es.index;

import com.dataparse.server.service.tasks.AbstractRequest;
import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexRequest extends AbstractRequest {

    private Long bookmarkId;
    private Boolean strictCheck;
    private boolean preserveHistory;
    private IngestErrorMode ingestErrorMode;
    private Descriptor descriptor;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return IndexTask.class;
    }
}
