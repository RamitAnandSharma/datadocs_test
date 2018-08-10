package com.dataparse.server.service.schema.log;

import com.dataparse.server.service.schema.*;
import lombok.*;

import javax.persistence.*;

@Data
@Entity
@DiscriminatorValue("r")
public class DataRemovedLogEntry extends BookmarkActionLogEntry {

    private Long size;

    private String accountId;
    private String externalId;

    public static DataRemovedLogEntry of(TableBookmark bookmark){
        DataRemovedLogEntry logEntry = new DataRemovedLogEntry();
        logEntry.setDatadocId(bookmark.getDatadoc().getId());
        logEntry.setBookmarkId(bookmark.getId());
        logEntry.setTableId(bookmark.getTableSchema().getId());
        return logEntry;
    }

}
