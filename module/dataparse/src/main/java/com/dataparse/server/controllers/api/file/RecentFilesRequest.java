package com.dataparse.server.controllers.api.file;

import lombok.*;

@Data
public class RecentFilesRequest {
    int limit = 10;
}
