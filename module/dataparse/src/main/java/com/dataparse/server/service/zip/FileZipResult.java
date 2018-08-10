package com.dataparse.server.service.zip;

import com.dataparse.server.service.tasks.TaskResult;
import com.dataparse.server.service.upload.FileDescriptor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileZipResult extends TaskResult {

    private Double complete;
    private FileDescriptor descriptor;

}
