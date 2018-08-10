package com.dataparse.server.controllers.api.table;


import lombok.*;

@Data
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
public class PresetBookmarkStateRequest extends AbstractBookmarkRequest {
    private boolean toCleanState;
}
