package com.dataparse.server.service.flow.node;

import com.dataparse.server.service.flow.settings.ColumnSettings;
import com.dataparse.server.service.flow.transform.Transform;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class Settings {

    private List<ColumnSettings> columns;
    private List<Transform> transforms;

}
