package com.dataparse.server.service.flow.settings;

import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

@Data
public class ColumnSettings implements Serializable {

    private String name;
    private boolean pkey;
    private TypeDescriptor type;

    private Integer index;
//    this is awful, but we need this, because of weird columns moving logic
    private Integer initialIndex;

    private String rename;
    private String annotation;

    private SearchType searchType;
    private ColFormat.Type formatType;

    private boolean disableFacets;

    private String splitOn;

    private boolean removeErrors;
    private String replaceErrors;

    private PreserveHistorySettings preserveHistory;

    private boolean removed;


    public void copyFrom(ColumnSettings settings){
        this.name = settings.name;
        this.pkey = settings.pkey;
        this.type = settings.type;
        this.index = settings.index;
        this.rename = settings.rename;

        this.searchType = settings.searchType;
        this.formatType = settings.formatType;

        this.disableFacets = settings.disableFacets;

        this.splitOn = settings.splitOn;

        this.removeErrors = settings.removeErrors;
        this.replaceErrors = settings.replaceErrors;

        this.preserveHistory = settings.preserveHistory;

        this.removed = settings.removed;
        this.initialIndex = settings.getInitialIndex();
        if(StringUtils.isNotBlank(settings.getAnnotation())) {
            this.annotation = settings.annotation;
        }
    }

}
