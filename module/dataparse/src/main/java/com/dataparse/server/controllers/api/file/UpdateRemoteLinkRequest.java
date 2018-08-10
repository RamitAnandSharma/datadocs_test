package com.dataparse.server.controllers.api.file;

import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.db.FileParams;
import lombok.Data;

import java.util.List;

@Data
public class UpdateRemoteLinkRequest extends AbstractCancellationRequest {

    String path;
    ConnectionParams connectionParams;
    FileParams fileParams;

    List<String> includeTables;
}
