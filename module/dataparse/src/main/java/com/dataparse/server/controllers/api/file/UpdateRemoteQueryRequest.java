package com.dataparse.server.controllers.api.file;

import lombok.Data;

@Data
public class UpdateRemoteQueryRequest {

    String path;
    String name;
    String query;

}
