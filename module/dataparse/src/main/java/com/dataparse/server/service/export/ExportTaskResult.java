package com.dataparse.server.service.export;

import com.dataparse.server.service.tasks.TaskResult;
import com.dataparse.server.service.upload.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExportTaskResult extends TaskResult {

    private Double complete;
    private Descriptor descriptor;
}
