package com.dataparse.server.service.schema.log;

import com.dataparse.server.service.schema.*;
import lombok.*;

import javax.persistence.*;

@Data
@Entity
@DiscriminatorValue("i")
public class DataIngestedLogEntry extends BookmarkActionLogEntry {

    private Long size;
    private Long rowsIngested;

    private String accountId;
    private String externalId;

    private String ingestTaskId; // ingest task ID in MongoDB

    public static DataIngestedLogEntry of(TableBookmark bookmark){
        DataIngestedLogEntry logEntry = new DataIngestedLogEntry();
        logEntry.setDatadocId(bookmark.getDatadoc().getId());
        logEntry.setBookmarkId(bookmark.getId());
        logEntry.setTableId(bookmark.getTableSchema().getId());
        return logEntry;
    }

}
