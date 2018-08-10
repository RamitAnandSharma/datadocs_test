package com.dataparse.server.controllers.api.share;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class PublicShareDatadocRequest {
    private Boolean enable;
    private Long datadocId;
}
