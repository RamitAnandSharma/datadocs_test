package com.dataparse.server.service.export;

import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.visualization.bookmark_state.state.QueryParams;
import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportTaskRequest extends AbstractRequest {

    /** ID of BQ's anonymous table */
    private String externalId;

    private Long datadocId;
    private ExportFormat format;
    private Long totalRowsCount;
    private List<Long> tableBookmarkIds;

    private QueryParams params;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return ExportTask.class;
    }
}
