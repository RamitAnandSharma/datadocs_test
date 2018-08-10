package com.dataparse.server.service.visualization.bookmark_state.state;

import com.dataparse.server.service.*;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import com.dataparse.server.service.visualization.bookmark_state.dto.BookmarkStateViewDTO;
import lombok.extern.slf4j.*;
import org.mongojack.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.net.*;
import java.util.*;
import java.util.stream.*;

@Slf4j
@Service
public class BookmarkStateRepository extends AbstractMongoRepository {

    private JacksonDBCollection<BookmarkState, BookmarkStateId> tabStateCollection;

    @PostConstruct
    public void init() throws UnknownHostException {
        super.init();
        tabStateCollection = JacksonDBCollection.wrap(
                database.getCollection("bookmark_state"), BookmarkState.class, BookmarkStateId.class);
    }

    public List<BookmarkState> getBookmarkStateList() {
        return tabStateCollection.find().toArray();
    }

    public void save(BookmarkState bookmarkState) {
        tabStateCollection.update(DBQuery.is("_id", bookmarkState.getBookmarkStateId()), bookmarkState, true, false);
    }

    public List<BookmarkStateViewDTO> getBookmarkStatesExceptDefault(Long tabId, UUID defaultStateId, Long userId) {
        DBQuery.Query query = DBQuery.is("tabId", tabId)
                .and(DBQuery.is("userId", userId))
                .and(DBQuery.notEquals("id", defaultStateId))
                .and(DBQuery.is("cleanState", false));
        return tabStateCollection.find(query)
                .toArray().stream()
                .map(BookmarkStateViewDTO::new)
                .collect(Collectors.toList());
    }

    public List<BookmarkState> getAllBookmarkStates(Long tabId, Long userId) {
        DBQuery.Query query = DBQuery.is("tabId", tabId)
                .and(DBQuery.is("userId", userId))
                .and(DBQuery.is("cleanState", false));
        return new ArrayList<>(tabStateCollection.find(query).toArray());
    }

    public List<BookmarkStateViewDTO> getAllSharedStates(Long tabId, Long userId) {
        DBQuery.Query query = DBQuery.is("tabId", tabId)
                .and(DBQuery.is("userId", userId))
                .and(DBQuery.is("shared", true));

        return tabStateCollection.find(query)
                .toArray().stream()
                .map(BookmarkStateViewDTO::new)
                .collect(Collectors.toList());
    }

    public List<BookmarkStateId> getAllStatesIdsByTabAndUserId(Long tabId, Long userId) {
        DBQuery.Query query = DBQuery.is("tabId", tabId)
                .and(DBQuery.is("userId", userId));
        return tabStateCollection.find(query).toArray()
                .stream()
                .map(BookmarkState::getBookmarkStateId)
                .collect(Collectors.toList());
    }

    public BookmarkState getBookmarkCleanStateId(Long tabId, Long userId) {
        DBQuery.Query query = DBQuery.is("tabId", tabId)
                .and(DBQuery.is("userId", userId))
                .and(DBQuery.is("cleanState", true));
        return tabStateCollection.findOne(query);
    }

    public BookmarkState findOne(BookmarkStateId stateId) {
        return tabStateCollection.findOne(DBQuery.is("_id", stateId));
    }

    public void remove(BookmarkState bookmarkState) {
        tabStateCollection.removeById(bookmarkState.getBookmarkStateId());
    }
    public void remove(BookmarkStateId stateId) {
        tabStateCollection.removeById(stateId);
    }
    public void remove(List<BookmarkStateId> stateIds) {
        stateIds.stream().forEach(this::remove);
    }


    public List<Long> getBookmarkIdsBySourceId(Long sourceId){
        return tabStateCollection.find(DBQuery.is("lastSourceSelectedId", sourceId), DBProjection.include("tabId"))
                .toArray().stream()
                .map(s -> s.getTabId())
                .collect(Collectors.toList());
    }
}
