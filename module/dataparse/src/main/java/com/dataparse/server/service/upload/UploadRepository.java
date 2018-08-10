package com.dataparse.server.service.upload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.transaction.Transactional;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.TermQuery;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.hibernate.internal.util.SerializationHelper;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.FullTextSession;
import org.hibernate.search.Search;
import org.hibernate.search.query.dsl.BooleanJunction;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.file.MoveRequest;
import com.dataparse.server.controllers.api.search.SearchRequest;
import com.dataparse.server.controllers.api.search.SearchResponse;
import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.db.DbQueryHistoryItem;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.engine.EngineSelectionStrategy;
import com.dataparse.server.service.entity.BasicInfrastructureEntity;
import com.dataparse.server.service.entity.EntityOperation;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.File;
import com.dataparse.server.service.files.Folder;
import com.dataparse.server.service.files.event.CreateFileEvent;
import com.dataparse.server.service.files.event.DeleteFileEvent;
import com.dataparse.server.service.files.event.UpdateFileEvent;
import com.dataparse.server.service.notification.Event;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.share.ShareRepository;
import com.dataparse.server.service.user.UserRepository;
import com.dataparse.server.util.FilenameUtils;
import com.dataparse.server.util.FunctionUtils;
import com.dataparse.server.util.db.OrderBy;
import com.dataparse.server.websocket.SockJSService;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
public class UploadRepository {

    private static final int MAX_QUERY_HISTORY_SIZE = 250;
    public static final String UPLOADS_TOPIC = "/upload-events";


    @Autowired
    private ShareRepository shareRepository;

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private SockJSService sockJSService;

    @Autowired
    private UserRepository userRepository;

    @Transactional
    public void refreshIndex(){
        Session session = sessionFactory.getCurrentSession();
        FullTextSession fullTextSession = Search.getFullTextSession(session);
        try {
            fullTextSession.createIndexer(Datadoc.class, File.class, Folder.class, Upload.class).startAndWait();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Transactional
    public Long resolveFileIdByPath(String pathString){
        return resolveFileIdByPath(pathString, false);
    }

    @Transactional
    public FileDescriptor getFileDescriptor(Long descriptorId) {
        String query = "select fd from FileDescriptor fd where fd.id = :id";
        return (FileDescriptor) sessionFactory
                .getCurrentSession()
                .createQuery(query)
                .setParameter("id", descriptorId)
                .getSingleResult();
    }

    @Transactional
    public Datadoc getDatadocByShareId(UUID shareId){
        String query = "select d from Datadoc d where d.shareId =:shareId";
        List<Datadoc> result = sessionFactory
                .getCurrentSession()
                .createQuery(query)
                .setParameter("shareId", shareId)
                .list();
        if(result.size() == 0) {
            return null;
        } else {
            return result.get(0);
        }
    }

    @Transactional
    public List<File> getFilesByUuid(String uuid) {
        String query = "select f " +
                "from File f " +
                "left join f.descriptor descriptor  " +
                "where descriptor.path = :path ";
        return (List<File>) sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("path", uuid).list();
    }

    @Transactional
    public Upload getUploadByChecksum(String checksum) {
        String query = "select u from Upload u where u.checksum = :checksum and u.user.id = :user and u.deleted = :false";
        return (Upload) sessionFactory.getCurrentSession()
                .createQuery(query)
                .setParameter("checksum", checksum)
                .setParameter("false", false)
                .setParameter("user", Auth.get().getUserId()).stream().findFirst().orElse(null);

    }

    private Long tryParseIdExpression(String expression){
        try {
            int separatorIdx = expression.indexOf(FilenameUtils.PATH_SEPARATOR);
            String idString = expression.substring(FilenameUtils.PATH_ID_PREFIX.length(), separatorIdx < 0 ? expression.length() : separatorIdx);
            if(!idString.isEmpty()) {
                return Long.parseLong(idString);
            }
            return null;
        } catch (Exception e){
            throw new RuntimeException("Can't parse file ID out of path string \"" + expression + "\"");
        }
    }

    private void moveToFolderInternal(Session session, AbstractFile file, Long folderId){
        Folder parent = null;
        if(folderId != null) {
            parent = session.get(Folder.class, folderId);
            if(parent == null){
                throw new RuntimeException("Can't find folder");
            } else if(parent.isDeleted()){
                throw new RuntimeException("Can't move file to deleted folder");
            }
        }
        file.setParent(parent);
    }

    private void renameFileInternal(Session session, AbstractFile file, String name){
        file.setName(name);
        ensureUniqueName(session, file);
    }

    @Transactional
    public Long resolveFileIdByPath(String pathString, boolean checkFolder){

        Long parentId = null;
        List<String> parentFolders = FilenameUtils.getParentFolders(pathString);
        if(!parentFolders.isEmpty() && parentFolders.get(0).startsWith(FilenameUtils.PATH_ID_PREFIX))
        {
            parentId = tryParseIdExpression(pathString);
            parentFolders = parentFolders.subList(1, parentFolders.size());
        }

        for (String name : parentFolders) {
            AbstractFile file = getFileByName(name, parentId);
            if (file == null) {
                throw new RuntimeException("File not found!");
            }
            parentId = file.getId();
        }
        String filename = FilenameUtils.getName(pathString);
        AbstractFile file;
        if(filename.isEmpty()){
            if(parentId == null) {
                return null;
            }
            file = getFile(parentId);
        } else {
            file = getFileByName(filename, parentId);
        }
        if(checkFolder && !(file instanceof Folder)){
            if(file == null){
                throw new RuntimeException("Folder not found!");
            }
            throw new RuntimeException("Provided location is not folder but a file");
        }
        if(file == null){
            throw new RuntimeException("File not found!");
        }
        return file.getId();
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<Upload> getUserUploads(Long userId){
        return sessionFactory.getCurrentSession()
                .createQuery("select u from Upload u left join u.descriptor d left join u.user us where u.deleted = false and d.path is not null and us.id = :userId")
                .setParameter("userId", userId)
                .list();
    }

    @Transactional
    public Folder createFolder(Long parentId, String folderName){
        Session session = sessionFactory.getCurrentSession();
        Folder folder = new Folder();
        folder.setCreated(new Date());
        folder.setUpdated(new Date());
        folder.setName(folderName);
        folder.setUser(userRepository.getUser(Auth.get().getUserId()));
        if(parentId != null){
            Folder parent = (Folder) session.get(Folder.class, parentId);
            folder.setParent(parent);
        }
        ensureUniqueName(session, folder);
        session.saveOrUpdate(folder);
        sockJSService.send(Auth.get(), UPLOADS_TOPIC, new CreateFileEvent(folder));
        return folder;
    }

    @Transactional
    public List<AbstractFile> moveFiles(MoveRequest moveRequest){
        return moveRequest.getFromPaths().stream()
                .map(path -> {
                    Long fromId = resolveFileIdByPath(path);
                    if (FilenameUtils.isPath(moveRequest.getToPath())) {
                        Long toId = resolveFileIdByPath(FilenameUtils.getPath(moveRequest.getToPath()), true);
                        return moveToFolderAndRename(fromId, toId, FilenameUtils.getName(moveRequest.getToPath()));
                    } else {
                        String newFileName = moveRequest.getToPath();
                        FilenameUtils.validateFilename(newFileName);
                        return renameFile(fromId, newFileName);
                    }
                })
                .collect(Collectors.toList());
    }

    @Transactional
    public AbstractFile moveToFolderAndRename(Long fileId, Long folderId, String newName){
        Session session = sessionFactory.getCurrentSession();
        AbstractFile file = (AbstractFile) session.get(AbstractFile.class, fileId);
        if(file.isDeleted()){
            throw new RuntimeException("Can't rename deleted file");
        }
        if(file instanceof Upload && ((Upload) file).getDescriptor().getFormat().options().isSection()){
            throw new RuntimeException("Can't remove section out of a composite data source");
        }
        AbstractFile oldFile = (AbstractFile) SerializationHelper.clone(file);
        moveToFolderInternal(session, file, folderId);
        if(StringUtils.isNotBlank(newName)) {
            renameFileInternal(session, file, newName);
        }
        ensureUniqueName(session, file);
        session.update(file);
        fetchSections(Collections.singletonList(file));
        sockJSService.send(Auth.get(), UPLOADS_TOPIC, new UpdateFileEvent(file, oldFile));
        return file;
    }

    @Transactional
    public void moveToFolder(Long fileId, Long folderId){
        moveToFolderAndRename(fileId, folderId, null);
    }

    @Transactional
    public AbstractFile renameFile(Long fileId, String name){
        Session session = sessionFactory.getCurrentSession();
        AbstractFile file = (AbstractFile) session.get(AbstractFile.class, fileId);
        if(file.isDeleted()){
            throw new RuntimeException("Can't rename deleted file");
        }
        AbstractFile oldFile = (AbstractFile) SerializationHelper.clone(file);
        renameFileInternal(session, file, name);
        session.saveOrUpdate(file);
        fetchSections(Collections.singletonList(file));
        sockJSService.send(Auth.get(), UPLOADS_TOPIC, new UpdateFileEvent(file, oldFile));
        return file;
    }

    private String getUniqueFileName(String name, List<String> fileNames){
        String baseName = org.apache.commons.io.FilenameUtils.getBaseName(name);
        String extension = org.apache.commons.io.FilenameUtils.getExtension(name);
        Pattern pattern = Pattern.compile(Pattern.quote(baseName)
                + "\\s\\(([0-9]+)\\)" + (StringUtils.isBlank(extension) ? "" : Pattern.quote("." + extension)));
        return getUniqueName(name, fileNames, pattern, (s, i) -> {
            String b = org.apache.commons.io.FilenameUtils.getBaseName(s);
            String e = org.apache.commons.io.FilenameUtils.getExtension(s);
            return b + " (" + i + ")" + (StringUtils.isBlank(e) ? "" : "." + extension);
        });
    }

    private String getUniqueFolderName(String name, List<String> folderNames){
        return getUniqueName(name, folderNames,
                Pattern.compile(Pattern.quote(name) + "\\s\\(([0-9]+)\\)"),
                (s, i) -> s + " (" + i + ")");
    }

    private String getUniqueName(String name, List<String> fileNames, Pattern pattern, BiFunction<String, Integer, String> addIndex){
        List<Integer> indexes = new ArrayList<>();
        for(String fileName : fileNames){
            if(fileName.equals(name)){
                indexes.add(0);
            } else {
                Matcher m = pattern.matcher(fileName);
                if(m.matches()){
                    String idx = m.group(1);
                    indexes.add(Integer.parseInt(idx));
                }
            }
        }
        if(indexes.isEmpty()){
            return name;
        }
        Collections.sort(indexes);
        for(int i = 0; i < indexes.size(); i++){
            if(!indexes.contains(i)){
                return i == 0 ? name : addIndex.apply(name, i);
            }
        }
        return addIndex.apply(name, indexes.size());
    }

    private void ensureUniqueName(Session session, AbstractFile file){
//        FilenameUtils.validateFilename(file.getName());

        String pattern, entity;
        if(file instanceof File) {
            pattern = org.apache.commons.io.FilenameUtils.getBaseName(file.getName()) + "%";
            entity = "File";
        } else if (file instanceof Folder) {
            pattern = file.getName() + "%";
            entity = "Folder";
        } else if (file instanceof Datadoc) {
            pattern = file.getName() + "%";
            entity = "Datadoc";
        } else {
            return;
        }

        String queryString = "select f.name from " + entity + " f join f.user u where u.id = :userId and f.deleted = false and f.name like :pattern";
        if (file.getId() != null) {
            queryString += " and f.id <> :fileId";
        }
        if (file.getParentId() != null){
            queryString += " and f.parent.id = :parentId";
        } else {
            queryString += " and f.parent is null";
        }
        Query query = session
                .createQuery(queryString)
                .setParameter("pattern", pattern)
                .setParameter("userId", file.getUser().getId());
        if (file.getId() != null) {
            query.setParameter("fileId", file.getId());
        }
        if (file.getParentId() != null){
            query.setParameter("parentId", file.getParentId());
        }
        List<String> fileNames = (List<String>) query.list();

        if(file instanceof File) {
            file.setName(getUniqueFileName(file.getName(), fileNames));
        } else {
            file.setName(getUniqueFolderName(file.getName(), fileNames));
        }
    }

    @Transactional
    public UploadSession getUploadSession(String sessionId){
        return sessionFactory.getCurrentSession().get(UploadSession.class, UUID.fromString(sessionId));
    }

    @Transactional
    public void deleteUploadSession(UploadSession uploadSession){
        sessionFactory.getCurrentSession().delete(uploadSession);
    }

    @Transactional
    public UploadSession createUploadSession(String fileKey){
        Session session = sessionFactory.getCurrentSession();
        UploadSession uploadSession = new UploadSession();
        uploadSession.setCreated(new Date());
        uploadSession.setKey(fileKey);
        uploadSession.setUser(userRepository.getUser(Auth.get().getUserId()));
        session.save(uploadSession);
        return uploadSession;
    }

    @Transactional
    public void saveFile(File file){
        if(file.isDeleted()){
            throw new RuntimeException("Can't save deleted upload");
        }
        Session session = sessionFactory.getCurrentSession();
        Event event = new CreateFileEvent(file);
        if(!(file.getParent() instanceof Upload)) {
            ensureUniqueName(session, file);
        }
        file.setCreated(new Date());
        file.setUpdated(new Date());
        file.getOrCreateEntityOperation(sessionFactory.getCurrentSession());
        if(file instanceof Upload) {
            Upload u = ((Upload) file);
            if(u.getSections() != null) {
                u.setSectionsSize(u.getSections().size());
            }
            if(!u.getDescriptor().isComposite()) {
                u.getDescriptor().setEngine(
                        EngineSelectionStrategy.current().getEngineType(u.getDescriptor()));
            }
        }
        session.save(file);
        if(file instanceof Upload) {
            Upload u = ((Upload) file);
            if(u.getSections() != null) {
                u.getSections().forEach(session::save);
            }
        }
        sockJSService.send(Auth.get(), UPLOADS_TOPIC, event);
    }


    @Transactional
    public void saveFiles(List<Upload> uploads) {
        Session session = sessionFactory.getCurrentSession();
        uploads.forEach(session::save);
    }

    @Transactional
    public void updateFile(AbstractFile file){
        Session session = sessionFactory.getCurrentSession();
        AbstractFile oldFile = getFile(file.getId());
        if(oldFile.isDeleted()){
            throw new RuntimeException("Can't update deleted upload");
        }
        file.setUpdated(new Date());
        file.setDeleted(false);
        session.evict(oldFile);
        Event event = new UpdateFileEvent(file, oldFile);
        if(!(file.getParent() instanceof Upload)) {
            ensureUniqueName(session, file);
        }
        if(file instanceof Upload){
            Upload upload = (Upload) file;
            if(upload.getSections() != null) {
                upload.setSectionsSize(upload.getSections().size());
            }
        }
        EntityOperation op = file.getOrCreateEntityOperation(sessionFactory.getCurrentSession());
        op.setModified(new Date());
        session.update(file);
        sockJSService.send(Auth.get(), UPLOADS_TOPIC, event);
    }


    @Transactional
    public void updateDescriptor(Descriptor descriptor){
        sessionFactory.getCurrentSession().update(descriptor);
    }

    @Transactional
    public DbDescriptor getDbDescriptorByConnectionParams(ConnectionParams connectionParams){
        Session currentSession = sessionFactory.getCurrentSession();
        List<DbDescriptor> result = (List<DbDescriptor>) currentSession.createQuery("SELECT r FROM DbDescriptor r where r.params.id = :id")
                .setParameter("id", connectionParams.getId())
                .list();
        if(result.size() == 1) {
            return result.get(0);
        } else {
            return null;
        }
    }



    @Transactional
    public void updateDescriptors(Collection<? extends Descriptor> descriptors){
        for (Descriptor descriptor : descriptors) {
            sessionFactory.getCurrentSession()
                    .update(descriptor);
        }
    }

    @Transactional
    public Long saveDescriptor(Descriptor descriptor){
        return (Long) sessionFactory.getCurrentSession().save(descriptor);
    }
    @Transactional
    public AbstractFile getFileByName(String name, Long parentId) {
        Criteria criteria = sessionFactory.getCurrentSession()
                .createCriteria(AbstractFile.class)
                .add(Restrictions.eq("deleted", false))
                .add(Restrictions.eq("name", name));
        if(parentId == null){
            criteria.add(Restrictions.isNull("parent"));
        } else {
            criteria.createAlias("parent", "p");
            criteria.add(Restrictions.eq("p.id", parentId));
        }
        try {
            return (AbstractFile) criteria.uniqueResult();
        } catch (HibernateException e) {
            throw new RuntimeException("Ambiguous results for name " + name + " of parent [ID=" + parentId + "]", e);
        }
    }

    @Transactional
    public AbstractFile getFile(Long fileId, boolean fetchParentPath, boolean fetchSections, boolean fetchRelatedDatadocs){
        AbstractFile file = sessionFactory.getCurrentSession()
                .get(AbstractFile.class, fileId);
        if(fetchParentPath){
            fetchParentPath(file);
        }
        if(fetchSections){
            fetchSections(Collections.singletonList(file));
        }
        if(fetchRelatedDatadocs){
            fetchDatadocs(Collections.singletonList(file));
        }
        return file;
    }

    @Transactional
    public AbstractFile getFile(Long fileId){
        return getFile(fileId, false, false, false);
    }

    @Transactional
    public void deleteUpload(Long fileId){
        AbstractFile file = (AbstractFile) sessionFactory.getCurrentSession()
                .get(AbstractFile.class, fileId);
        file.setDeleted(true);
        sessionFactory.getCurrentSession().update(file);
        sockJSService.send(Auth.get(), UPLOADS_TOPIC, new DeleteFileEvent(file));
    }

    @Transactional
    public void deleteUploads(List<Long> fileIds){
        String updateQuery = "UPDATE Upload u set deleted = :true where u.id in (:ids)";

        sessionFactory.getCurrentSession()
                .createQuery(updateQuery)
                .setParameter("true", true)
                .setParameterList("ids", fileIds)
                .executeUpdate();
    }

    @Transactional
    public void deleteUploadsByUserId(Long userId){
        String updateQuery = "UPDATE Upload u set deleted = :true where u.user.id = :userId";

        sessionFactory.getCurrentSession()
                .createQuery(updateQuery)
                .setParameter("true", true)
                .setParameter("userId", userId)
                .executeUpdate();
    }

    @Transactional
    public void fetchParentPath(AbstractFile file){
        AbstractFile currentFile = file;
        List<Map<String, Object>> parents = new ArrayList<>();
        while(currentFile.getParent() != null){
            AbstractFile parent = currentFile.getParent();
            Map<String, Object> parentMap = new HashMap<>();
            parentMap.put("type", parent.getType());
            parentMap.put("name", parent.getName());
            parentMap.put("id", currentFile.getParentId());
            parents.add(parentMap);
            currentFile = parent;
        }
        Collections.reverse(parents);
        file.setParentsPath(parents);
    }

    @Transactional
    @SuppressWarnings("unchecked")
    private void fetchSections(List<? extends AbstractFile> files){
        List<Upload> uploads = new ArrayList<>();
        for(AbstractFile file : files){
            if(file instanceof Upload){
                Upload upload = (Upload) file;
                if(upload.getDescriptor().getFormat().options().isComposite()){
                    upload.setSections(new ArrayList<>());
                    uploads.add(upload);
                }
            }
        }
        if(!uploads.isEmpty()) {
            List<Long> ids = uploads.stream()
                    .map(u -> u.getId())
                    .collect(Collectors.toList());
            List<Upload> sections = sessionFactory.getCurrentSession()
                    .createCriteria(Upload.class)
                    .add(Restrictions.eq("deleted", false))
                    .createAlias("parent", "p")
                    .add(Restrictions.in("p.id", ids))
                    .list();

            Multimap<Long, Upload> uploadSections = HashMultimap.create();

            for(Upload section : sections){
                if(section.getParent() instanceof Upload) {
                    Long parentId = section.getParent().getId();
                    if (uploads.contains(section.getParent())) {
                        uploadSections.put(parentId, section);
                    }
                }
            }

            for(Upload upload : uploads){
                if(uploadSections.containsKey(upload.getId())){
                    upload.getSections().addAll(uploadSections.get(upload.getId()));
                }
            }
        }
    }

    @Transactional
    @SuppressWarnings("unchecked")
    private void fetchDatadocs(List<AbstractFile> files){
        List<Upload> uploads = files.stream()
                .filter(f -> f instanceof Upload)
                .map(f -> {
                    Upload u = (Upload) f;
                    u.setRelatedDatadocs(new ArrayList<>());
                    return u;
                })
                .collect(Collectors.toList());

        if(!uploads.isEmpty()){
            List<Long> ids = uploads.stream()
                    .map(u -> u.getId())
                    .collect(Collectors.toList());

            List<Object[]> datadocs = sessionFactory.getCurrentSession()
                    .createQuery("select u.id, d from TableBookmark b " +
                                 "join b.datadoc d " +
                                 "join b.tableSchema s " +
                                 "join s.uploads u " +
                                 "where d.deleted <> :true and u.id in :uploadIds")
                    .setParameterList("uploadIds", ids)
                    .setParameter("true", true)
                    .list();
            Multimap<Long, Datadoc> uploadDatadocs = HashMultimap.create();
            for(Object[] tuples: datadocs){
                uploadDatadocs.put((Long) tuples[0], (Datadoc) tuples[1]);
            }

            for(Upload upload : uploads){
                if(uploadDatadocs.containsKey(upload.getId())){
                    upload.setRelatedDatadocs(new ArrayList<>(uploadDatadocs.get(upload.getId())));
                }
            }
        }
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<Upload> getSources(List<Long> sourceIds){
        if(sourceIds.isEmpty()){
            return new ArrayList<>();
        }
        return sessionFactory.getCurrentSession()
                .createQuery("select u from Upload u where u.id in :sourceIds")
                .setParameterList("sourceIds", sourceIds)
                .list();
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<AbstractFile> getFiles(Collection<Long> fileIds){
        if(fileIds.isEmpty()){
            return new ArrayList<>();
        }
        return sessionFactory.getCurrentSession()
                .createQuery("select u from AbstractFile u where u.id in :fileIds")
                .setParameterList("fileIds", fileIds)
                .list();
    }

//todo use thymeleaf for base query
    @SuppressWarnings("unchecked")
    @Transactional
    public List<AbstractFile> getFiles(Collection<Long> ids, Long userId, Long parentId,
                                       int offset, int limit,
                                       OrderBy orderBy,
                                       boolean fetchCompositeSections,
                                       boolean fetchRelatedDatadocs,
                                       boolean sourcesOnly,
                                       boolean foldersOnly,
                                       boolean datadocsOnly){
        boolean orderByLastViewedByMe = orderBy != null && "lastViewedByMe".equals(orderBy.getField());
        boolean orderByLastViewedByMeOrAddedOn = orderBy != null && "lastViewedByMeOrAddedOn".equals(orderBy.getField());
        String entityViewedSubquery = ", (select max(eo2.viewed) from EntityOperation eo2 where eo2 in eo and eo2.user.id = :userId) as lastViewedByMe ";
        String entityViewedOrAddedOnSubquery = ", (select coalesce(max(case when f.class = 'Datadoc' then eo2.viewed else f.created end), f.created) from EntityOperation eo2 where eo2 in eo and eo2.user.id = :userId) as lastViewedByMeOrAddedOn ";
        String folderFirst = "CASE "+
                "WHEN f.entityType = 'Folder' THEN 0 " +
                "ELSE 1 " +
                "END ";

        boolean isName = orderBy != null && "name".equals(orderBy.getField());
        String additionalSortingCondition = orderBy != null && isName ? "" : ", upper(f.name), f.id";

        if(isName) {
            orderBy.setWrapFieldWith("upper");
        }

        List<String> entityTypes = new ArrayList<>();

        if (sourcesOnly) {
            entityTypes.add("Upload");
        }
        if (foldersOnly) {
            entityTypes.add("Folder");
        }
        if (datadocsOnly) {
            entityTypes.add("Datadoc");
        }

        String query = "select f " +
                       (orderByLastViewedByMe ? entityViewedSubquery : "") +
                       (orderByLastViewedByMeOrAddedOn ? entityViewedOrAddedOnSubquery : "") +
                       "from " + "AbstractFile" + " f " +
                       "left join f.descriptor descriptor " +
                       "left join f.user user " +
                       "left join f.entityOperation eo on eo.user.id = :userId " +
                       "where f.deleted = :false " +
                       " and (f.user.id = :userId or f.id in (select usf.primaryKey.datadoc.id from UserFileShare usf where usf.primaryKey.user.id = :userId)) " +
                       "and f.parent " + (parentId == null ? "is null " : ".id = " + parentId + " ") +
                       (ids.isEmpty() ? "" : " and f.id in :ids ") +
                       (entityTypes.isEmpty() ? "" : " and f.class in :entityTypes ") +
                       "order by " +
                       (orderBy == null
                        ? folderFirst
                        : folderFirst + ", " + orderBy.toSqlString() + " nulls last ")
                       + additionalSortingCondition;
        // name, type, added on, added by, size, linked datadocs
        org.hibernate.query.Query q = sessionFactory.getCurrentSession()
                .createQuery(query)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .setParameter("false", false)
                .setParameter("userId", userId);
        q = entityTypes.isEmpty() ? q : q.setParameter("entityTypes", entityTypes);
        q = ids.isEmpty() ? q : q.setParameter("ids", ids);
        List result = q.list();

        List<AbstractFile> items;
        if(orderByLastViewedByMe || orderByLastViewedByMeOrAddedOn){
            items = (List<AbstractFile>) result.stream()
                    .map(o -> ((Object[]) o)[0])
                    .collect(Collectors.toList());
        } else {
            items = result;
        }
        for(AbstractFile item : items) {
            fetchParentPath(item);
        }
        if(fetchCompositeSections){
            fetchSections(items);
        }
        if(fetchRelatedDatadocs){
            fetchDatadocs(items);
        }
        return items;
    }

    @Transactional
    public void saveQueryHistoryItem(Long descriptorId, DbQueryHistoryItem item){
        // todo omg look at this java code intended only to keep max history size...
        DbDescriptor descriptor = sessionFactory.getCurrentSession().get(DbDescriptor.class, descriptorId);
        descriptor.getQueryHistory().sort(
                Collections.reverseOrder(
                        Comparator.comparing(DbQueryHistoryItem::getStartTime)));
        if(descriptor.getQueryHistory().size() >= MAX_QUERY_HISTORY_SIZE){
            List<DbQueryHistoryItem> tmp = new ArrayList<>(descriptor.getQueryHistory().subList(0, MAX_QUERY_HISTORY_SIZE - 1));
            descriptor.getQueryHistory().clear();
            descriptor.getQueryHistory().addAll(tmp);
        }
        descriptor.getQueryHistory().add(item);
        sessionFactory.getCurrentSession().saveOrUpdate(descriptor);
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<AbstractFile> getFiles(Long userId, Long parentId){
        return getFiles(Lists.newArrayList(), userId, parentId, 0, Integer.MAX_VALUE, null, false, false, false, false, false);
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<AbstractFile> getRecentFiles(Long userId, int maxResults){
        List<AbstractFile> result = sessionFactory.getCurrentSession().createQuery("select f from AbstractFile f " +
                                                       "left join f.parent p " +
                                                       "where f.deleted = false " +
                                                       "and f.user.id = :userId " +
                                                       "and (p.id is null or p.class <> 'Upload') " +
                                                       "and f.class in ('File', 'Upload') " +
                                                       "order by f.updated desc")
                .setParameter("userId", userId)
                .setMaxResults(maxResults)
                .list();
        for(AbstractFile file : result) {
            fetchParentPath(file);
        }
        fetchDatadocs(result);
        return result;
    }

    private org.apache.lucene.search.Query createSearchQuery(Class<? extends BasicInfrastructureEntity> clazz, FullTextSession fullTextSession, Long userId, String searchString, boolean sources){
        String escapedSearchString = searchString.replaceAll("[^A-Za-z0-9]", " ").trim();
        if(StringUtils.isBlank(searchString)){
            return null;
        }

        QueryBuilder qb = fullTextSession.getSearchFactory()
                .buildQueryBuilder().forEntity(clazz)
                .get();

        Analyzer analyzer = fullTextSession.getSearchFactory().getAnalyzer(Upload.class);
        QueryParser parser = new QueryParser("name", analyzer);
        String[] tokenized;
        BooleanJunction matchNameOrExactName = qb.bool();
        BooleanJunction matchKeywords = qb.bool();
        if(!StringUtils.isBlank(escapedSearchString)) {
            try {
                org.apache.lucene.search.Query query = parser.parse(escapedSearchString);
                String cleanedText = query.toString("name");
                tokenized = cleanedText.split("\\s");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            for (String aTokenized : tokenized) {
                if (StringUtils.isNotBlank(aTokenized)) {
                    matchNameOrExactName.must(qb.keyword().wildcard().onField("name")
                                                      .matching("*" + aTokenized + "*").createQuery());
                }
            }
        }
        matchNameOrExactName.should(qb.keyword().wildcard().onField("exact_name").boostedTo(1.5f).matching("*" + searchString + "*").createQuery());
        matchNameOrExactName.should(qb.keyword().onField("exact_name").boostedTo(2).matching(searchString).createQuery());
        matchKeywords.should(qb.keyword().wildcard().onField("keywords").boostedTo(0.5f).matching(searchString).createQuery());

        BooleanJunction q = qb.bool().must(qb.bool()
                               .should(matchNameOrExactName.createQuery())
                               .should(matchKeywords.createQuery())
                               .createQuery())
                .must(new TermQuery(new Term("userIdStr", String.valueOf(userId))));

        if(!sources){
            q = q.must(new TermQuery(new Term("type", "section-ds"))).not();
        }
        return q.createQuery();
    }


    @Transactional
    @SuppressWarnings("unchecked")
    public List<AbstractFile> autoSuggestFilesAndDocs(Long userId, String searchString, Integer limit){
        Session session = sessionFactory.getCurrentSession();
        FullTextSession fullTextSession = Search.getFullTextSession(session);

        org.apache.lucene.search.Query q = createSearchQuery(BasicInfrastructureEntity.class, fullTextSession, userId, searchString, false);
        if(q == null){
            return new ArrayList<>();
        }

        FullTextQuery ftq = fullTextSession.createFullTextQuery(q, AbstractFile.class);
        if(limit > 0){
            ftq.setMaxResults(100);
        }
        List<AbstractFile> result = ((List<AbstractFile> ) ftq.list()).stream()
                .filter(FunctionUtils.complement(AbstractFile::isDeleted))
                .collect(Collectors.toList());
        for(AbstractFile file : result) {
            fetchParentPath(file);
        }
        return result.subList(0, Math.min(result.size(), limit));
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<AbstractFile> autoSuggestSources(Long userId, String searchString, Integer limit) {
        Session session = sessionFactory.getCurrentSession();
        FullTextSession fullTextSession = Search.getFullTextSession(session);

        org.apache.lucene.search.Query q = createSearchQuery(File.class, fullTextSession, userId, searchString, false);
        if (q == null) {
            return new ArrayList<>();
        }

        FullTextQuery ftq = fullTextSession.createFullTextQuery(q, File.class);
        if (limit > 0) {
            ftq.setMaxResults(limit);
        }
        return ((List<AbstractFile> ) ftq.list()).stream()
                .filter(FunctionUtils.complement(AbstractFile::isDeleted))
                .collect(Collectors.toList());
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public SearchResponse searchFilesAndDocs(Long userId, SearchRequest sr){
        Integer limit = sr.getLimit();
        Session session = sessionFactory.getCurrentSession();
        FullTextSession fullTextSession = Search.getFullTextSession(session);

        org.apache.lucene.search.Query q = createSearchQuery(BasicInfrastructureEntity.class, fullTextSession, userId, sr.getS(), true);
        if(q == null){
            return new SearchResponse(Lists.newArrayList());
        }

        FullTextQuery fileSearch = fullTextSession.createFullTextQuery(q, AbstractFile.class);

        if(limit > 0){
            fileSearch.setMaxResults(limit);
        }

        List<AbstractFile> files = ((List<AbstractFile>) fileSearch.list()).stream().filter(FunctionUtils.complement(AbstractFile::isDeleted)).collect(Collectors.toList());
        if(sr.getFetchSections()){
            fetchSections(files);
        }
        if(sr.getFetchRelatedDatadocs()){
            fetchDatadocs(files);
        }

        return new SearchResponse(files);
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<RemoteLinkDescriptor> getRemoteLinkDescriptors(){
        // todo get not deleted upload descriptors only
        return sessionFactory.getCurrentSession()
                .createCriteria(RemoteLinkDescriptor.class)
                .list();
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<DbDescriptor> getDisconnectedDescriptors(){
        return (List<DbDescriptor>) sessionFactory.getCurrentSession()
                .createQuery("SELECT dd FROM DbDescriptor dd where valid = :false")
                .setParameter("false", false).list();
    }
    @Transactional
    @SuppressWarnings("unchecked")
    public List<Upload> getDbUploads(){
        return (List<Upload>) sessionFactory.getCurrentSession()
                .createQuery("SELECT u FROM Upload u where u.deleted = :false and u.descriptor.class = :descClass")
                .setParameter("false", false)
                .setParameter("descClass", DbDescriptor.class.getSimpleName())
                .list();
    }

    @Transactional
    public Datadoc getDatadocById(Long id) {
        String query = "select d from Datadoc d where d.id = :id";
        return (Datadoc) sessionFactory
                .getCurrentSession()
                .createQuery(query)
                .setParameter("id", id).uniqueResult();
    }

    @Transactional
    public Upload getUploadById(Long id) {
        String query = "select d from Upload d where d.id = :id";
        Upload upload = (Upload) sessionFactory
                .getCurrentSession()
                .createQuery(query)
                .setParameter("id", id).uniqueResult();
        List<Upload> uploads = Lists.newArrayList(upload);
        fetchSections(uploads);
        return uploads.size() == 1 ? uploads.get(0) : null;
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public Map<Long, Collection<Long>> getDatadocsForRemoteUpload(List<Long> uploadIds){
        Multimap<Long, Long> uploadDatadocs = HashMultimap.create();
        List<Object[]> tuples = sessionFactory.getCurrentSession()
                .createQuery("select d.id, u.id from TableBookmark b " +
                             "join b.datadoc d join b.tableSchema ts join ts.uploads u " +
                             "where d.deleted = :false and u.id in (:uploadIds)")
                .setParameterList("uploadIds", uploadIds)
                .setParameter("false", false)
                .list();
        for(Object[] tuple: tuples){
            uploadDatadocs.put((Long) tuple[1], (Long) tuple[0]);
        }
        return uploadDatadocs.asMap();
    }

    @Transactional
    @SuppressWarnings("unchecked")
    public List<TableBookmark> getBookmarksByUpload(Long uploadId, Class clazz){
        return sessionFactory.getCurrentSession()
                .createQuery("select b from TableBookmark b " +
                             "join b.datadoc d " +
                             "join b.tableSchema ts " +
                             "join ts.uploads u " +
                             "where d.deleted = :false " +
                                   "and u.id = (:upload) " +
                                   "and ts.descriptor.class = :descriptorType")
                .setParameter("upload", uploadId)
                .setParameter("descriptorType", clazz.getSimpleName())
                .setParameter("false", false)
                .list();
    }
}