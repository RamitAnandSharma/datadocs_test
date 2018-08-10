package com.dataparse.server.service.flow.node;

import lombok.*;

@Data
@EqualsAndHashCode(callSuper = true)
public class DbTableInputNodeSettings extends InputNodeSettings {

    private Long tableId;

}
