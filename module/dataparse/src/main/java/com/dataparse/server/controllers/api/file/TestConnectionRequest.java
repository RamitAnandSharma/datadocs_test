package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.db.ConnectionParams;
import lombok.Data;

@Data
public class TestConnectionRequest extends AbstractCancellationRequest {
    String path;
    ConnectionParams connectionParams;
}
