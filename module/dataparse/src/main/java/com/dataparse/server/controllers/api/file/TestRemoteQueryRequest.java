package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.db.ConnectionParams;
import lombok.Data;

@Data
public class TestRemoteQueryRequest {

    String query;
    ConnectionParams connectionParams;
}
