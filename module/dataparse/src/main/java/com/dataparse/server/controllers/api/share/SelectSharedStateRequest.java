package com.dataparse.server.controllers.api.share;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SelectSharedStateRequest {
    private Long datadocId;
    private UUID sharedStateId;
}