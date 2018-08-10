package com.dataparse.server.controllers.api.file;

import lombok.Data;

@Data
public class UpdateSchemaFromBookmarkRequest {
    private Long sourceId;
    private Long tabId;
}
