package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.flow.settings.*;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.hibernate.internal.util.*;
import org.mongojack.*;

import java.io.*;
import java.util.*;

/**
 * Holds visualization page state.
 * Should be immutable, so that after event is applied it returns new copy of state.
 * All nested fields must be Serializable in order for copy method to work.
 */
@Data
@NoArgsConstructor
public class BookmarkState implements Serializable {

    public BookmarkState(List<Col> columnList){
        this.columnList = columnList;
    }

    @Id
    private BookmarkStateId bookmarkStateId;

    private Boolean changeable = false;
    private String stateName;
    private Long userId;
    private Long tabId;
    private UUID id;
    private Boolean cleanState = false;
    private Long revision;
    private List<Notification> notifications = new ArrayList<>();
    private Boolean convertToUserTimezone = false;
    private TimeZone timezone = TimeZone.getTimeZone("Etc/Universal"); // UTC
    private ColNameFormat colNameFormat = ColNameFormat.OFF; // UTC
    private boolean shared = false;

    // ingest
    private String flowJSON;
    private String pendingFlowJSON; // todo named incorrectly, should switch roles with flowJSON
    private FlowSettings flowSettings = new FlowSettings();
    private PageMode pageMode = PageMode.INGEST;
    private Long lastSourceSelectedId;
    private int queryEditorHeight;

    // visualization
    private boolean instantSearch = true;
    private boolean autoRefresh = true;
    private boolean refreshAvailable = false;
    private boolean highlightMatches = true;

    private Map<String, Boolean> rowsCollapsedState = new HashMap<>();
    private Map<String, Integer> rowsHeight = new HashMap<>();
    private Integer defaultRowHeight;
    private Integer pinnedRowsCount = 0;
    private Integer pinnedColsCount = 0;

    private List<Col> columnList = new ArrayList<>();

    /** dictionary of tags */
    private List<Show> showList = new ArrayList<>();
    private List<Agg> aggList = new ArrayList<>();
    private List<Sort> sortList = new ArrayList<>();
    private List<Sort> aggSortList = new ArrayList<>();
    private List<Sort> pivotSortList = new ArrayList<>();
    private List<RawShow> rawShowList = new ArrayList<>();

    /** filters that should be refreshed after pendingQueryParams applied */
    private Set<String> filtersToRefresh = new HashSet<>();

    private QueryParams queryParams = new QueryParams();
    private QueryParams pendingQueryParams = new QueryParams();
    private QueryParams beforeViewRawParams = new QueryParams();

    private ViewMode viewMode = ViewMode.TABLE;

    private Boolean visible = true;
    @Deprecated
    private Boolean showAggregationTotal = false;
    private Boolean showTotals = false;
    private Boolean showFilters = true;
    private Map<String, Boolean> pivotCollapsedState = new HashMap<>();
    private String lastSelectedCurrency;

    public BookmarkState copy(){
        BookmarkState clone = (BookmarkState) SerializationHelper.clone(this);
//todo reset all not shared fields here
        clone.getNotifications().clear();
        clone.setCleanState(false);
        return clone;
    }

    /* This getter is exceptionally for internal use,
       Jackson should not serialize it. */
    public UUID getId() {
        return this.bookmarkStateId.getStateId();
    }

    public Long getTabId() {
        return this.bookmarkStateId.getTabId();
    }

    public Long getUserId() {
        return this.bookmarkStateId.getUserId();
    }

    @JsonIgnore
    public BookmarkStateId getBookmarkStateId() {
        return this.bookmarkStateId;
    }

    @JsonIgnore
    public QueryParams getCurrentQueryParams(){
        if(autoRefresh){
            return queryParams;
        } else {
            return pendingQueryParams;
        }
    }

}
