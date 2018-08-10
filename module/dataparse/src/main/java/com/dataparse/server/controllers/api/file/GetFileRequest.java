package com.dataparse.server.controllers.api.file;

import lombok.*;
import lombok.experimental.Builder;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GetFileRequest {
    String path;

    boolean sections = false;
    boolean relatedDatadocs = false;
}
