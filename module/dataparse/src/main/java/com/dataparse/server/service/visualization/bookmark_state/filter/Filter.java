package com.dataparse.server.service.visualization.bookmark_state.filter;

import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.fasterxml.jackson.annotation.*;
import lombok.*;

import java.io.*;
import java.util.*;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.MINIMAL_CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
public class Filter implements Serializable {

    public Filter(String field){
        this.field = field;
    }

    public Filter(String field, List<FilterValue> list){
        this(field);
        this.listMode = true;
        this.list = list;
    }

    String field;

    boolean hidden;
    boolean selected;

    Long cardinality = -1L;

    boolean listMode;
    List<FilterValue> list = new ArrayList<>();
    boolean showSearch = true;
    String search;
    boolean and_or; // todo rename
    boolean linlog = true;

    @JsonIgnore
    public boolean isActive() {
        return !hidden && selected && list.stream().anyMatch(o -> o.isSelected() || !o.isShow());
    }

}
