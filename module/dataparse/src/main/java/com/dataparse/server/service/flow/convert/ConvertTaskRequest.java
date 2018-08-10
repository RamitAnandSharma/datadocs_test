package com.dataparse.server.service.flow.convert;

import com.dataparse.server.service.parser.*;
import com.dataparse.server.service.tasks.*;
import lombok.*;

@Data
public class ConvertTaskRequest extends AbstractRequest {

    private String path;
    private DataFormat toFormat;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return ConvertTask.class;
    }
}
