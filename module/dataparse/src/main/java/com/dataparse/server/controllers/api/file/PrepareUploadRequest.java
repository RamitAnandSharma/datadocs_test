package com.dataparse.server.controllers.api.file;

import lombok.*;
import lombok.experimental.Builder;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PrepareUploadRequest {

    private int count;

}
