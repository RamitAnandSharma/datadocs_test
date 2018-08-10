package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.Data;

import java.io.Serializable;

@Data
public class ShowSettings implements Serializable {

    private Integer width;

    private Sort sort;

}
