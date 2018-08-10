package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.*;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class AggSettings implements Serializable {

    public AggSettings(final AggSort sort) {
        this.sort = sort;
    }

    private Boolean showTotal = true;
    private Integer limit;

    private AggSort sort = new AggSort();

}
