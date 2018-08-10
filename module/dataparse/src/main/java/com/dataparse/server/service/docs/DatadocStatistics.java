package com.dataparse.server.service.docs;

import lombok.*;

import java.util.*;

@Data
public class DatadocStatistics {

    private Long datadocId;
    private String datadocName;
    private Integer sheets;
    private Long totalRows;
    private String storageTypes;
    private Long storageSpace;
    private Long dataIngested;
    private Long dataProcessed;
    private Date created;
    private Long createdByUserId;
    private String createdByUserName;
    private Date lastSaved;

    private List<BookmarkStatistics> bookmarks;

}
