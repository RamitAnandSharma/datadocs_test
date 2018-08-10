package com.dataparse.server.service.schema;


import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.table.*;
import com.dataparse.server.controllers.exception.ForbiddenException;
import com.dataparse.server.service.docs.*;
import com.dataparse.server.service.embed.EmbedSettings;
import com.dataparse.server.service.engine.*;
import com.dataparse.server.service.entity.*;
import com.dataparse.server.service.files.event.DeleteFileEvent;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.upload.refresh.RefreshType;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.service.visualization.bookmark_state.*;
import com.dataparse.server.service.visualization.bookmark_state.dto.BookmarkStateViewDTO;
import com.dataparse.server.service.visualization.bookmark_state.state.*;
import com.dataparse.server.util.ApiUtils;
import com.dataparse.server.websocket.SockJSService;
import lombok.extern.slf4j.*;
import org.apache.commons.lang3.*;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.*;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class TableRepository {

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private BookmarkStateStorage bookmarkStateStorage;

    @Autowired
    private BookmarkStateRepository bookmarkStateRepository;

    @Autowired
    private SockJSService sockJSService;

    @Autowired
    private UserRepository userRepository;

    @Transactional
    public TableSchema getTableSchema(Long id){
        return sessionFactory.getCurrentSession()
                .get(TableSchema.class, id);
    }

    @Transactional
    public void saveTableSchema(TableSchema tableSchema){
        sessionFactory.getCurrentSession()
                .saveOrUpdate(tableSchema);
    }

    @Transactional
    public TableSchema getSchemaByBookmarkId(Long bookmarkId){
        return (TableSchema) sessionFactory.getCurrentSession()
                .createQuery("select s from TableBookmark b left join b.tableSchema s where b.id = :bookmarkId")
                .setParameter("bookmarkId", bookmarkId)
                .uniqueResult();
    }

    @Transactional
    public void saveDatadoc(Datadoc datadoc){
        EntityOperation op = datadoc.getOrCreateEntityOperation(sessionFactory.getCurrentSession());
        op.setModified(new Date());
        datadoc.setUpdated(new Date());
        datadoc.setName(StringUtils.substring(datadoc.getName(), 0, 255));
        sessionFactory.getCurrentSession().saveOrUpdate(datadoc);
    }

    @Transactional
    public int cleanUpPreSavedDatadocs(DateTime currentDate) {
        return sessionFactory.getCurrentSession().createQuery("UPDATE AbstractFile set deleted = :true " +
                "WHERE deleted <> :true AND preSaved = :true AND created < :date ")
                .setParameter("true", true)
                .setParameter("date", currentDate.minusDays(1).toDate())
                .executeUpdate();
    }


    @Transactional
    public Datadoc getDatadoc(Long id){
        Datadoc datadoc = sessionFactory.getCurrentSession()
                .get(Datadoc.class, id);
        ApiUtils.checkExisting(id, datadoc);
        return datadoc;
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public void deleteDatadoc(Long id){
        Datadoc datadoc = getDatadoc(id);
        datadoc.setDeleted(true);
        sessionFactory.getCurrentSession().update(datadoc);
        sockJSService.send(Auth.get(), UploadRepository.UPLOADS_TOPIC, new DeleteFileEvent(datadoc));
    }

    @Transactional
    public List<BookmarkStateViewDTO> getDatadocSharedStates(Long datadocId) {
        List<TableBookmark> bookmarks = getTableBookmarks(datadocId);
        List<BookmarkStateViewDTO> sharedStates = new ArrayList<>();

        bookmarks.forEach(bookmark -> {
            List<BookmarkStateViewDTO> bookmarkSharedStates
                    = bookmarkStateRepository.getAllSharedStates(bookmark.getId(), Auth.get().getUserId());
            if(bookmarks.size() > 1) {
                bookmarkSharedStates.forEach(sharedState -> sharedState.setName(bookmark.getName() + " – " + sharedState.getName()));
            }
            sharedStates.addAll(bookmarkSharedStates);
        });
        return sharedStates;
    }

    @Transactional
    public void updateViewed(Long datadocId){
        Datadoc datadoc = sessionFactory.getCurrentSession().get(Datadoc.class, datadocId);
        EntityOperation op = datadoc.getOrCreateEntityOperation(sessionFactory.getCurrentSession());
        op.setViewed(new Date());
        sessionFactory.getCurrentSession().update(datadoc);
    }


    @Transactional
    @SuppressWarnings("unchecked")
    public Set<String> getUserSchemas(Long userId, EngineType engineType){
        return new HashSet<>(sessionFactory.getCurrentSession()
                .createQuery("select ts.externalId from TableBookmark b" +
                             " join b.datadoc d  join d.user u" +
                             " left join b.tableSchema ts join ts.descriptor descr " +
                             " where u.id = :userId and ts.engineType = :engineType and d.deleted = :false")
                .setParameter("engineType", engineType)
                .setParameter("userId", userId)
                .setParameter("false", false)
                .list());
    }

    private void checkUniqueName(Session session, TableBookmark tableBookmark){
        String queryString = "select count(*) from TableBookmark b join b.datadoc d join d.user u where u.id = :userId and b.name = :name and d.id = :datadocId ";
        if(tableBookmark.getId() != null) {
            queryString += " and b.id != :id ";
        }
        Query query = session.createQuery(queryString)
                .setParameter("name", tableBookmark.getName())
                .setParameter("userId", tableBookmark.getDatadoc().getUser().getId())
                .setParameter("datadocId", tableBookmark.getDatadoc().getId());
        if(tableBookmark.getId() != null){
            query.setParameter("id", tableBookmark.getId());
        }
        Long duplicates = (Long) query.uniqueResult();
        if(duplicates > 0){
            throw new RuntimeException("Bookmark name must be unique");
        }
    }

    @Transactional
    private String getNewTableBookmarkName(Datadoc datadoc){
        List<String> bookmarkNames = getTableBookmarks(datadoc.getId()).stream()
                .map(TableBookmark::getName).collect(Collectors.toList());
        String name = "Sheet";
        List<Long> tabNumbers = new ArrayList<>();
        for(String bookmarkName : bookmarkNames){
            try {
                tabNumbers.add(Long.parseLong(bookmarkName.substring(5)));
            } catch (Exception e){}
        }
        tabNumbers.add(datadoc.getTabCounter());
        Collections.sort(tabNumbers);
        long i = tabNumbers.get(tabNumbers.size() - 1) + 1;
        datadoc.setTabCounter(i);
        return name + i;
    }

    @Transactional
    public TableBookmark initTableBookmark(String name, Long bookmarkToCopyId){
        TableBookmark tableBookmark = new TableBookmark();
        tableBookmark.setCreated(new Date());
        tableBookmark.setUpdated(new Date());
        tableBookmark.setCreatedByUser(userRepository.getUser(Auth.get().getUserId()));
        tableBookmark.setName(name);
        EmbedSettings embedSettings = new EmbedSettings();
        embedSettings.setTitle(tableBookmark.getName());
        embedSettings.setUuid(UUID.randomUUID().toString());
        if(bookmarkToCopyId != null) {
            // clone schema
            TableSchema schema = getTableBookmark(bookmarkToCopyId).getTableSchema();
            schema.setId(null);
            sessionFactory.getCurrentSession().detach(schema);
            tableBookmark.setTableSchema(schema);
        } else {
            TableSchema schema = new TableSchema();
            schema.setRefreshSettings(RefreshSettings.never());
            tableBookmark.setTableSchema(schema);
        }
        saveTableSchema(tableBookmark.getTableSchema());

        tableBookmark.setEmbedSettings(embedSettings);
        return tableBookmark;
    }

    @Transactional
    public TableBookmark saveBookmark(TableBookmark tableBookmark, Long datadocId, BookmarkState bookmarkState) {
        Session session = sessionFactory.getCurrentSession();
        Datadoc datadoc = getDatadoc(datadocId);
        tableBookmark.setDatadoc(datadoc);
        tableBookmark.setName(tableBookmark.getName() == null ? getNewTableBookmarkName(datadoc) : tableBookmark.getName());
        Long position = (Long) session
                .createQuery("select count(tb) from TableBookmark tb where tb.datadoc.id = :datadocId")
                .setParameter("datadocId", datadocId)
                .uniqueResult();
        tableBookmark.setPosition(position.intValue());
        tableBookmark.setDefaultState(bookmarkState.getId());
        tableBookmark.setState(bookmarkState);
        session.saveOrUpdate(tableBookmark);
        return tableBookmark;
    }

    @Transactional
    public TableBookmark updateTableBookmark(Long tableBookmarkId, UpdateTableBookmarkRequest request){
        Session session = sessionFactory.getCurrentSession();
        TableBookmark oldBookmark = getTableBookmark(tableBookmarkId);
        if(oldBookmark == null){
            throw new RuntimeException("Bookmark " + tableBookmarkId + " doesn't exist");
        }
        oldBookmark.setName(request.getName());
        checkUniqueName(session, oldBookmark);
        session.update(oldBookmark);
        return oldBookmark;
    }

    @Transactional
    public TableBookmark updateTableBookmark(TableBookmark bookmark) {
        sessionFactory.getCurrentSession().update(bookmark);
        return bookmark;
    }

    @Transactional
    public Datadoc updateDatadoc(Datadoc datadoc) {
        sessionFactory.getCurrentSession().update(datadoc);
        return datadoc;
    }

    @Transactional
    public EmbedSettings updateTableBookmarkEmbedSettings(Long tableBookmarkId, EmbedSettings embedSettings){
        EmbedSettings oldSettings = getTableBookmark(tableBookmarkId).getEmbedSettings();
        embedSettings.setId(oldSettings.getId());
        embedSettings.setTableBookmark(oldSettings.getTableBookmark());
        embedSettings.setUuid(oldSettings.getUuid());
        return (EmbedSettings) sessionFactory.getCurrentSession().merge(embedSettings);
    }

    @Transactional
    public EmbedSettings getTableBookmarkEmbedSettings(String uuid){
        return (EmbedSettings) sessionFactory.getCurrentSession()
                .createCriteria(EmbedSettings.class)
                .add(Restrictions.eq("uuid", uuid))
                .uniqueResult();
    }

    /*warning impure method*/
    private void fetchBookmarkState(List<TableBookmark> bookmarks){
        for(TableBookmark bookmark: bookmarks) {
            try {
                fetchBookmarkState(bookmark, bookmark.getBookmarkStateId());
            } catch (Exception e){
                log.warn("Can't fetch bookmark state", e);
            }
        }
    }

    /*warning impure method*/
    private void fetchBookmarkState(TableBookmark bookmark, BookmarkStateId stateId) {
        boolean isDefaultState = bookmark.getDefaultState().equals(stateId.getStateId());
        BookmarkState state = bookmarkStateStorage.get(stateId, true, isDefaultState).getState();
        List<BookmarkStateViewDTO> bookmarksStates = bookmarkStateRepository.getBookmarkStatesExceptDefault(stateId.getTabId(), bookmark.getDefaultState(), stateId.getUserId());
        bookmark.setState(state);
        bookmark.setAllBookmarkStates(bookmarksStates);
    }

    private void presetSpecificState(TableBookmark bookmark, UUID stateId) {
        Long tabId = bookmark.getBookmarkStateId().getTabId();
        boolean isDefault = bookmark.getDefaultState().equals(stateId);
        BookmarkState state = bookmarkStateStorage.get(new BookmarkStateId(tabId, stateId), true, isDefault).getState();
        List<BookmarkStateViewDTO> bookmarksStates = bookmarkStateRepository.getBookmarkStatesExceptDefault(tabId, bookmark.getDefaultState(), Auth.get().getUserId());
        bookmark.setState(state);
        bookmark.setAllBookmarkStates(bookmarksStates);
    }

    @Transactional
    public List<TableBookmark> getTableBookmarks(Long datadocId) {
        return getTableBookmarks(datadocId, false);
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<Long> getTableBookmarkIds(Long datadocId){
        return sessionFactory.getCurrentSession()
                .createQuery("select b.id from TableBookmark b join b.datadoc d where d.id = :datadocId")
                .setParameter("datadocId", datadocId)
                .list();
    }

    @Transactional
    public List<Upload> getAttachedSources(Long datadocId) {
        return sessionFactory.getCurrentSession()
                .createQuery("select u from TableBookmark b " +
                        "join b.datadoc d " +
                        "join b.tableSchema s " +
                        "join s.uploads u " +
                        "where d.deleted <> :true and b.datadoc.id = :datadocId")
                .setParameter("datadocId", datadocId)
                .setParameter("true", true)
                .list();
    }

    @Transactional
    public List<Long> getTableBookmarkIds(List<Long> datadocIds){
        if(datadocIds.isEmpty()){
            return new ArrayList<>();
        }
        return sessionFactory.getCurrentSession()
                .createQuery("select b.id from TableBookmark b join b.datadoc d where d.id = :datadocIds")
                .setParameterList("datadocIds", datadocIds)
                .list();
    }

    @Transactional
    public List<BookmarkStateId> getBookmarksCurrentState(Long datadocId, List<Long> bookmarks) {
        String query = "select tb from TableBookmark tb " +
                "where tb.id in (:bookmarks) and tb.datadoc.id = :datadoc";

        List<TableBookmark> result = sessionFactory.getCurrentSession().createQuery(query)
                .setParameter("datadoc", datadocId)
                .setParameterList("bookmarks", bookmarks)
                .list();
        return result.stream().map(TableBookmark::getBookmarkStateId).collect(Collectors.toList());
    }

    @Transactional
    public List<TableBookmark> getTableBookmarksWithSpecificState(Long datadocId, Long tabId, UUID stateId, boolean fetchState) {
        List<TableBookmark> bookmarks = sessionFactory.getCurrentSession()
                .createCriteria(TableBookmark.class)
                .createAlias("datadoc", "d")
                .add(Restrictions.eq("d.id", datadocId))
                .list();
        Optional<TableBookmark> currentBookmark = bookmarks.stream().filter(bookmark -> bookmark.getId().equals(tabId)).findFirst();
        if(currentBookmark.isPresent()) {
            TableBookmark tableBookmark = currentBookmark.get();
//          todo fix this
            tableBookmark.setCurrentState(stateId);
            tableBookmark.getBookmarkStateId().setStateId(stateId);
        }
        if(fetchState) {
            fetchBookmarkState(bookmarks);
        }
        Collections.sort(bookmarks, (o1, o2) -> new Integer(o1.getPosition()).compareTo(o2.getPosition()));
        return bookmarks;
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<TableBookmark> getTableBookmarks(Long datadocId, boolean fetchState){
        List<TableBookmark> bookmarks = sessionFactory.getCurrentSession()
                .createCriteria(TableBookmark.class)
                .createAlias("datadoc", "d")
                .add(Restrictions.eq("d.id", datadocId))
                .list();
        if(fetchState) {
            fetchBookmarkState(bookmarks);
        }
        Collections.sort(bookmarks, (o1, o2) -> new Integer(o1.getPosition()).compareTo(o2.getPosition()));
        return bookmarks;
    }

    @Transactional
    public void saveCurrentBookmarkState(Long tabId) {
        TableBookmark bookmark = sessionFactory.getCurrentSession()
                .get(TableBookmark.class, tabId);
        fetchBookmarkState(Collections.singletonList(bookmark));
    }

    @Transactional
    public void removeBookmarkState(Long tabId, UUID stateId) {
        TableBookmark bookmark = sessionFactory.getCurrentSession().get(TableBookmark.class, tabId);
        if(bookmark.getDefaultState().equals(stateId)) {
            throw new ForbiddenException("Can not remove default state. ");
        }
        BookmarkStateId st = new BookmarkStateId(tabId, stateId);
        BookmarkStateHistoryWrapper state = bookmarkStateStorage.get(st, false);
        bookmarkStateStorage.evict(st);
        bookmarkStateRepository.remove(state.getState());
    }

    @Transactional
    public TableBookmark getTableBookmark(Long tabId, UUID stateId) {
        if(stateId == null) {
            return getTableBookmark(tabId);
        }
        TableBookmark bookmark = sessionFactory.getCurrentSession()
                .get(TableBookmark.class, tabId);
        presetSpecificState(bookmark, stateId);
        return bookmark;
    }

    @Transactional
    public TableBookmark getTableBookmarkForUser(Long tableBookmarkId, Long userId) {
        TableBookmark bookmark = sessionFactory.getCurrentSession()
                .get(TableBookmark.class, tableBookmarkId);
        if(bookmark != null) {
            sessionFactory.getCurrentSession().detach(bookmark);
            BookmarkStateId stateId = new BookmarkStateId(tableBookmarkId, bookmark.getDefaultState(), userId);
            fetchBookmarkState(bookmark, stateId);
        }
        return bookmark;
    }

    @Transactional
    public TableBookmark getTableBookmark(Long tableBookmarkId){
        return getTableBookmark(tableBookmarkId, true);
    }

    @Transactional
    public TableBookmark getTableBookmark(Long tableBookmarkId, Boolean fetchState){
        TableBookmark bookmark = sessionFactory.getCurrentSession()
                .get(TableBookmark.class, tableBookmarkId);
        if(bookmark != null && fetchState){
            try {
                if(bookmark.getTableSchema().getCommitted() != null) {
                    fetchBookmarkState(Collections.singletonList(bookmark));
                }
            } catch (Exception e){
                log.error("Error while fetching bookmark {}", tableBookmarkId);
            }
        }
        return bookmark;
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public void removeTableBookmark(Long tableBookmarkId){
        TableBookmark tableBookmark = getTableBookmark(tableBookmarkId);
        List<TableBookmark> bookmarks = getTableBookmarks(tableBookmark.getDatadoc().getId());
        int position = tableBookmark.getPosition();
        for(TableBookmark bookmark: bookmarks){
            if(bookmark.getPosition() > position){
                bookmark.setPosition(bookmark.getPosition() - 1);
                sessionFactory.getCurrentSession().merge(bookmark);
            }
        }
        sessionFactory.getCurrentSession().delete(tableBookmark);
    }

    @Transactional
    public void moveTableBookmark(Long tableBookmarkId, int position){
        TableBookmark tableBookmark = getTableBookmark(tableBookmarkId);
        List<TableBookmark> bookmarks = getTableBookmarks(tableBookmark.getDatadoc().getId());
        bookmarks.remove(tableBookmark);
        bookmarks.add(position, tableBookmark);
        for(TableBookmark bookmark: bookmarks){
            bookmark.setPosition(bookmarks.indexOf(bookmark));
            sessionFactory.getCurrentSession().merge(bookmark);
        }
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<TableBookmark> getScheduledBookmarks(){
        return sessionFactory.getCurrentSession()
                .createQuery("select b from TableBookmark b join b.datadoc d join b.tableSchema t join t.refreshSettings s where s.type <> :noneType and d.deleted = :false")
                .setParameter("noneType", RefreshType.NONE)
                .setParameter("false", false)
                .list();
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public Map<String, Long> getTablesPerAccount(List<String> accounts){
        List<Object[]> tuples = sessionFactory.getCurrentSession()
                .createQuery("select t.accountId, count(t) from TableSchema t where t.engineType = :engineType group by t.accountId")
                .setParameter("engineType", EngineType.BIGQUERY)
                .list();
        Map<String, Long> result = new HashMap<>();
        accounts.forEach(account -> {
            result.put(account, 0L);
        });

        for(Object[] tuple : tuples){
            String accountId = (String) tuple[0];
            Long count = (Long) tuple[1];
            if(accounts.contains(accountId)){
                result.put(accountId, count);
            }
        }
        return result;
    }

}
