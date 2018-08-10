package com.dataparse.server.service.db;

import lombok.*;

import javax.persistence.*;
import java.io.Serializable;
import java.util.*;

@Data
@Entity
public class DbQueryHistoryItem implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(columnDefinition = "TEXT")
    private String query;
    private Date startTime;
    private long duration;
    private boolean success;

}
