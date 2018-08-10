package com.dataparse.server.service.visualization.bookmark_state.state;

import com.fasterxml.jackson.annotation.*;
import lombok.*;

import java.io.*;
import java.util.*;

@Data
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class Sort implements Serializable {

    public Sort() {
        this.direction = SortDirection.ASC;
        this.priority = 0;
    }

    public Sort(SortDirection direction){
        this.direction = direction;
    }

    public Sort(RawShow rawShow) {
        this.priority = rawShow.getSort().getPriority();
        this.direction = rawShow.getSort().getDirection();
    }

    SortDirection direction;

    Integer priority;

}
