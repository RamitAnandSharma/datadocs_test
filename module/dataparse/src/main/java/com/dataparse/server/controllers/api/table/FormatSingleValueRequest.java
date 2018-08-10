package com.dataparse.server.controllers.api.table;

import com.dataparse.server.service.visualization.bookmark_state.state.ColFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FormatSingleValueRequest {
    private Long tabId;
    private UUID stateId;
    private String fieldName;
    private ColFormat format;
}
