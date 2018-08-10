package com.dataparse.server.service.docs;

import lombok.*;

import java.util.*;

@Data
public class BookmarkStatistics {

    private Long bookmarkId;
    private String bookmarkName;
    private Long rows;
    private Integer columns;
    private String storageType;
    private Long storageSpace;
    private Long dataIngested;
    private Long dataProcessed;
    private Date created;
    private Long createdByUserId;
    private String createdByUserName;
    private Date lastSaved;
    private Long lastSavedByUserId;
    private String lastSavedByUserName;
    private boolean createdAndLastSavedAreSameToHours; // sorry for this
}
