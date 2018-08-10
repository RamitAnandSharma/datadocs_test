package com.dataparse.server.controllers.api.share;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class AbstractSingleShareRequest {
    private Long userId;
    private Long datadocId;
}
