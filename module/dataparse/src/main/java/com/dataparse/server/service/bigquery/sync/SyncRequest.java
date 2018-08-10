package com.dataparse.server.service.bigquery.sync;

import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SyncRequest extends AbstractRequest {

    private Long bookmarkId;
    private String accountId;
    private Descriptor descriptor;
    private IngestErrorMode ingestErrorMode;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return SyncTask.class;
    }
}
