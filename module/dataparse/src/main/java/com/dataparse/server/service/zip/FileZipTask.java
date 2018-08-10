package com.dataparse.server.service.zip;

import com.dataparse.server.service.tasks.AbstractTask;
import com.dataparse.server.service.tasks.TaskState;
import com.dataparse.server.service.upload.FileDescriptor;
import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

@Data
@Slf4j
public class FileZipTask extends AbstractTask<FileZipRequest>{

    @Autowired
    private FileZipService fileZipService;

    @Override
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(FileZipTaskState.IN_PROGRESS, () -> {
            List<Long> fileIds = getRequest().getFileIds();

            FileZipResult result = new FileZipResult();
            setResult(result);

            log.info("Start preparing download of files {}", fileIds);
            FileDescriptor descriptor = fileZipService.zip(getRequest().getAuth().getUserId(), fileIds, (complete) -> {
                result.setComplete(complete);
                FileZipTask.this.saveState();
            });
            result.setDescriptor(descriptor);
            log.info("Finished preparing download of files {}", fileIds);
        });
    }

    @Override
    public void cancel() {
        // do nothing
    }
}
