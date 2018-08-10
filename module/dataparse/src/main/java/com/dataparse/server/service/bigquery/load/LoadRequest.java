package com.dataparse.server.service.bigquery.load;

import com.dataparse.server.service.tasks.*;
import com.dataparse.server.service.upload.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoadRequest extends AbstractRequest {

    private Long tableId;
    private String accountId;
    private Descriptor descriptor;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return LoadTask.class;
    }
}
