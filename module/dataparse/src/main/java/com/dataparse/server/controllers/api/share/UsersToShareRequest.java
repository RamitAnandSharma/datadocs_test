package com.dataparse.server.controllers.api.share;

import lombok.Data;

@Data
public class UsersToShareRequest {
    private Long datadocId;
    private String namePart;
}
