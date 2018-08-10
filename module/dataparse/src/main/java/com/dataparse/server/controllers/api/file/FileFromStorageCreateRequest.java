package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.storage.StorageType;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileFromStorageCreateRequest {
    private String path;
    private String filePath;
    private String bufferPath;
    private StorageType storageType;
    private String contentType;
    private DataFormat format;
    private String originalFileName;
    Long fileSize;
}
