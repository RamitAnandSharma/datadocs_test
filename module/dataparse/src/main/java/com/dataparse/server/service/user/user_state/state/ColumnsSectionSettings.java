package com.dataparse.server.service.user.user_state.state;

import lombok.Data;

@Data
public class ColumnsSectionSettings {
    private Integer selected = 0;
    private SortSettings sortSettings = new SortSettings();
}
