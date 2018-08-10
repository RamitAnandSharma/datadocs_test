package com.dataparse.server.service.flow.node;

import lombok.*;

@Data
@EqualsAndHashCode(callSuper = true)
public class DbQueryInputNodeSettings extends InputNodeSettings {

    private Long queryId;
    private String query;

}
