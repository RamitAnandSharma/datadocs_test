package com.dataparse.server.controllers.api.file;

import com.dataparse.server.util.db.*;
import lombok.*;
import lombok.experimental.Builder;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ListFilesRequest {

    private int offset;
    private int limit;
    private OrderBy orderBy;
    private String path;
    private boolean withSections = false;
    private boolean withDatadocs = false;

    private boolean sourcesOnly = false;
    private boolean foldersOnly = false;
    private boolean datadocsOnly = false;
}
