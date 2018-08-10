package com.dataparse.server.service.visualization.bookmark_state.state;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggSort extends Sort {

    public AggSort() {
        this.type = SortType.BY_KEY;
        this.direction = SortDirection.ASC;
        this.isCount = false;
    }

    public AggSort(final SortDirection direction, final Boolean isCount) {
        this.type = SortType.BY_KEY;
        this.direction = direction;
        this.isCount = isCount;
    }

    public AggSort(final SortDirection direction, final Boolean isCount, final String field, final List<Object> aggKeyPath) {
        this.type = SortType.BY_VALUE;
        this.direction = direction;
        this.field = field;
        this.aggKeyPath = aggKeyPath;
        this.isCount = isCount;
    }

    SortType type;

    String field;

    List<Object> aggKeyPath;

    Boolean isCount = false;
}
