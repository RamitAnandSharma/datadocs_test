package com.dataparse.server.controllers.api.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SaveBookmarkStateRequest {
    private String name;
    private Long tabId;
    private UUID stateId;
}
