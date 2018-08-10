package com.dataparse.server.controllers.api.file;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PrepareUploadResponse {
    private String sessionId;
    private String accessToken;
}
