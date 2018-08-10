package com.dataparse.server.controllers.api.table;

import lombok.Data;

@Data
public class CreateTableBookmarkRequest extends AbstractBookmarkRequest {

    private Long datadocId;

}
