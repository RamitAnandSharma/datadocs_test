package com.dataparse.server.controllers.api.file;

import lombok.Data;

@Data
public class RenameRequest {
    String path;
    String name;
}
