package com.dataparse.server.service.visualization.bookmark_state.state;

import lombok.*;

import java.io.*;
import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Notification implements Serializable {

    private String id;
    private Date date;
    private int type;
    private String message;

    public Notification(final Date date, final int type, final String message) {
        this.id = UUID.randomUUID().toString();
        this.date = date;
        this.type = type;
        this.message = message;
    }
}
