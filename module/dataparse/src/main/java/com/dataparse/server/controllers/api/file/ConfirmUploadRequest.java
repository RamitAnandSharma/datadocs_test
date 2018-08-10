package com.dataparse.server.controllers.api.file;

import lombok.*;
import lombok.experimental.Builder;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ConfirmUploadRequest {

    private String sessionId;
    private String path;
    private String fileName;
    private long fileSize;
    private String contentType;
    private String hash;
    private String fileId;

}
