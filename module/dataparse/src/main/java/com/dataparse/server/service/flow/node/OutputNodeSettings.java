package com.dataparse.server.service.flow.node;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class OutputNodeSettings extends Settings {

    private Long bookmarkId;
    private boolean preserveHistory;
    private String parentTaskId;

}
