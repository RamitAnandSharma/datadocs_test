package com.dataparse.server.service.zip;

import com.dataparse.server.service.tasks.AbstractRequest;
import com.dataparse.server.service.tasks.AbstractTask;
import lombok.Data;

import java.util.List;

@Data
public class FileZipRequest extends AbstractRequest {

    private List<Long> fileIds;

    @Override
    public Class<? extends AbstractTask> getTaskClass() {
        return FileZipTask.class;
    }
}
