package com.dataparse.server.controllers.api.file;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Builder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SaveFileResponse {
    private String path;
    private Long descriptorId;
}
