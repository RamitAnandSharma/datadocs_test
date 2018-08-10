package com.dataparse.server.controllers.api.file;

import lombok.Data;

@Data
public class CreateRemoteQueryRequest{
    String name;
    String query;

    String remoteLinkPath;
}
