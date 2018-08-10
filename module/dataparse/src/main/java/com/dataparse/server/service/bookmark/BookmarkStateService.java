package com.dataparse.server.service.bookmark;

import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkBuilderFactory;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateBuilder;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateStorage;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import com.dataparse.server.service.visualization.bookmark_state.state.Col;
import com.dataparse.server.util.ListUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BookmarkStateService {

    @Autowired
    private BookmarkBuilderFactory bookmarkBuilderFactory;

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;


    public Boolean isStateAcceptableForNewDescriptor(Descriptor newDescriptor, BookmarkState bookmarkState) {
        List<ColumnInfo> newColumns = newDescriptor.getColumns();
        Set<String> missedColumnsAliases = defineMissedColumns(bookmarkState, newColumns).stream().map(Col::getField).collect(Collectors.toSet());

        return checkMissedColumnInShowMeSection(missedColumnsAliases, bookmarkState) &&
                checkMissedColumnInAggregationSection(missedColumnsAliases, bookmarkState) &&
                checkMissedColumnInFiltersSection(missedColumnsAliases, bookmarkState) &&
                checkMissedColumnInPivotSection(missedColumnsAliases, bookmarkState);
    }

    public void modifyStatesForUser(Descriptor descriptor, List<BookmarkStateId> stateIds) {
        stateIds.forEach(stateId -> {
            BookmarkState state = bookmarkStateStorage.get(stateId, true, true).getState();
            Boolean stateAcceptable = isStateAcceptableForNewDescriptor(descriptor, state);
            if(stateAcceptable) {
                BookmarkState newState = updateState(descriptor, state);
                bookmarkStateStorage.evict(newState.getBookmarkStateId());
                bookmarkStateStorage.init(newState, true);
            } else {
                bookmarkStateStorage.evict(stateId);
            }
        });
    }

    public BookmarkState updateState(Descriptor newDescriptor, BookmarkState sourceState) {
        BookmarkStateBuilder bookmarkStateBuilder = bookmarkBuilderFactory.create(sourceState.getTabId());
        BookmarkState newState = sourceState.copy();

        Map<String, ColumnInfo> newColumnsByName = ListUtils.groupByKey(newDescriptor.getColumns(), ColumnInfo::getName);

        List<Col> missedColumns = defineMissedColumns(newState, newDescriptor.getColumns());
        List<ColumnInfo> newColumns = defineNewColumns(newColumnsByName, newState);

        // add and remove cols
        BookmarkStateBuilder.removeColumnsByNativeOne(newState, missedColumns);
        bookmarkStateBuilder.addColumns(newState, newColumns);

        updateAliasesInAllPlacesIfNeeded(newState, newColumnsByName);

        return newState;
    }

    /**
     * impure
     */
    public void updateAliasesInAllPlacesIfNeeded(BookmarkState bookmarkState, Map<String, ColumnInfo> newColumnsByName) {
        Map<String, String> newColNamesByOld = getColsNewAliasesByOld(newColumnsByName, bookmarkState);
        if(newColNamesByOld.size() > 0) {
            bookmarkState.getColumnList().forEach(col -> col.setField(newColNamesByOld.getOrDefault(col.getField(), col.getField())));
            bookmarkState.getShowList().forEach(show -> show.setField(newColNamesByOld.getOrDefault(show.getField(), show.getField())));
            bookmarkState.getAggList().forEach(agg -> agg.setField(newColNamesByOld.getOrDefault(agg.getField(), agg.getField())));
            bookmarkState.getRawShowList().forEach(rawShow -> rawShow.setField(newColNamesByOld.getOrDefault(rawShow.getField(), rawShow.getField())));

            bookmarkState.getQueryParams().getShows().forEach(show -> show.setField(newColNamesByOld.getOrDefault(show.getField(), show.getField())));
            bookmarkState.getQueryParams().getAggs().forEach(agg -> agg.setField(newColNamesByOld.getOrDefault(agg.getField(), agg.getField())));
            bookmarkState.getQueryParams().getPivot().forEach(pivot -> pivot.setField(newColNamesByOld.getOrDefault(pivot.getField(), pivot.getField())));
            bookmarkState.getQueryParams().getFilters().forEach(filter -> filter.setField(newColNamesByOld.getOrDefault(filter.getField(), filter.getField())));
        }
    }


    public Map<String, String> getColsNewAliasesByOld(Map<String, ColumnInfo> columns, BookmarkState sourceState) {
        List<Col> stateCols = new ArrayList<>(sourceState.getColumnList());
        Map<String, String> result = new HashMap<>();

        stateCols.forEach(col -> {
            String oldName = col.getOriginalField();
            String oldAlias = col.getField();
            ColumnInfo columnInfo = columns.get(oldName);
            if(columnInfo != null && !oldAlias.equals(columnInfo.getAlias())) {
                result.put(oldAlias, columnInfo.getAlias());
            }
        });

        return result;
    }

    public List<ColumnInfo> defineNewColumns(Map<String, ColumnInfo> newColumnsByName, BookmarkState state) {
        Set<String> oldColumnsNames = state.getColumnList().stream()
                .map(Col::getOriginalField)
                .collect(Collectors.toSet());

        Map<String, ColumnInfo> copiedMap = new HashMap<>(newColumnsByName);
        copiedMap.keySet().removeAll(oldColumnsNames);
        return new ArrayList<>(copiedMap.values());
    }



    private List<Col> defineMissedColumns(BookmarkState bookmarkState, List<ColumnInfo> newColumns) {
        return bookmarkState.getColumnList().stream()
                .filter(oldColumn -> {
                    String oldOriginalName = oldColumn.getOriginalField();
                    DataType oldDataType = oldColumn.getType();
                    return newColumns.stream()
                            .noneMatch(newColumn ->
                                    newColumn.getName().equals(oldOriginalName) && newColumn.getType().getDataType().equals(oldDataType));
                }).collect(Collectors.toList());
    }

    private Boolean checkMissedColumnInFiltersSection(Set<String> missedColumnsAliases, BookmarkState bookmarkState) {
        return bookmarkState.getQueryParams().getFilters()
                .stream()
                .noneMatch(filter -> missedColumnsAliases.contains(filter.getField()) && filter.isActive());
    }

    private Boolean checkMissedColumnInPivotSection(Set<String> missedColumnsAliases, BookmarkState bookmarkState) {
        return bookmarkState.getQueryParams().getPivot()
                .stream()
                .noneMatch(filter -> missedColumnsAliases.contains(filter.getField()));
    }

    private Boolean checkMissedColumnInAggregationSection(Set<String> missedColumnsAliases, BookmarkState bookmarkState) {
        return bookmarkState.getQueryParams().getAggs()
                .stream()
                .noneMatch(agg -> missedColumnsAliases.contains(agg.getField()));
    }

    private Boolean checkMissedColumnInShowMeSection(Set<String> missedColumnsAliases, BookmarkState bookmarkState) {
        return bookmarkState.getQueryParams().getShows()
                .stream()
                .noneMatch(show -> missedColumnsAliases.contains(show.getField()));
    }


}
