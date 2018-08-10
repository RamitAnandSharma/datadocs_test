package com.dataparse.server.controllers.api.file;

import lombok.Data;

import java.util.List;

@Data
public class PrepareDownloadRequest {
    List<String> paths;
}
