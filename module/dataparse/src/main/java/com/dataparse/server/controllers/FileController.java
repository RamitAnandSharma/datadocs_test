package com.dataparse.server.controllers;

import com.dataparse.server.auth.Auth;
import com.dataparse.server.controllers.api.file.*;
import com.dataparse.server.controllers.exception.ValidationException;
import com.dataparse.server.service.common.CancellationRequestService;
import com.dataparse.server.service.db.ConnectionParams;
import com.dataparse.server.service.db.DBService;
import com.dataparse.server.service.db.TableInfo;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.files.AbstractFile;
import com.dataparse.server.service.files.File;
import com.dataparse.server.service.files.Folder;
import com.dataparse.server.service.flow.FlowExecutionResult;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.security.DatadocActionAccessibility;
import com.dataparse.server.service.security.DatadocSecurityService;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.storage.IStorageStrategy;
import com.dataparse.server.service.tasks.TaskInfo;
import com.dataparse.server.service.tasks.TaskManagementService;
import com.dataparse.server.service.tasks.TasksRepository;
import com.dataparse.server.service.upload.*;
import com.dataparse.server.service.zip.FileZipRequest;
import com.dataparse.server.service.zip.FileZipResult;
import com.dataparse.server.service.zip.FileZipTask;
import com.dataparse.server.util.ApiUtils;
import com.dataparse.server.util.ExceptionUtils;
import com.dataparse.server.util.FilenameUtils;
import com.dataparse.server.util.JsonUtils;
import com.dataparse.server.util.thread.SilentThreadWithRetries;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/files")
@Slf4j
@Api(value = "Files", description = "Managing of files and remote links")
public class FileController extends ApiController {

    @Autowired
    private UploadService uploadService;

    @Autowired
    private TaskManagementService taskManagementService;

    @Autowired
    private DatadocSecurityService securityService;

    @Autowired
    private CancellationRequestService cancellationService;

    @Autowired
    private TasksRepository tasksRepository;

    @Autowired
    private UploadRepository uploadRepository;

    @Autowired
    private IStorageStrategy storageStrategy;

    @Autowired
    private TableService tableService;

    @Autowired
    private DBService dbService;


    @ApiOperation("Retrieve related queries and tables")
    @GetMapping(value = "/related_source_data/{sourceId}")
    public List<RelatedDatabaseSourceData> getRelatedSourceData(@PathVariable Long sourceId) {
        Descriptor descriptor = uploadRepository.getUploadById(sourceId).getDescriptor();
        if(descriptor instanceof DbDescriptor) {
            List<TableBookmark> bookmarksByUpload = uploadRepository.getBookmarksByUpload(sourceId, DbQueryDescriptor.class);
            return bookmarksByUpload.stream().map(bookmark -> {
                RemoteLinkDescriptor removeDescriptor = (RemoteLinkDescriptor) bookmark.getTableSchema().getDescriptor();
                return new RelatedDatabaseSourceData(bookmark.getDatadoc().getName() + "." + bookmark.getName(), removeDescriptor);
            }).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }


    @ApiOperation("Get description of file or folder")
    @RequestMapping(value = "/get_file", method = RequestMethod.POST)
    public AbstractFile getFile(@RequestBody GetFileRequest getFileRequest) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Long id = uploadRepository.resolveFileIdByPath(getFileRequest.getPath());
        if (id == null) {
            throw new RuntimeException("File not found!");
        }
        AbstractFile file = uploadRepository.getFile(id, true, getFileRequest.isSections(), getFileRequest.isRelatedDatadocs());
        ApiUtils.checkExisting(id, file);

        if(file instanceof Datadoc) {
            Datadoc datadoc = (Datadoc) file;
            if(datadoc.getCommitted() == null && !datadoc.getUser().getId().equals(Auth.get().getUserId())) {
                List<TaskInfo> tasks = tasksRepository.getPendingFlowTasksAsListByDatadoc(datadoc.getUser().getId(), datadoc.getId());
                double averagePercentage = tasks.stream().mapToDouble(task -> {
                    FlowExecutionResult result = (FlowExecutionResult) task.getResult();
                    Double percentComplete = result.getState().get("OUTPUT").getPercentComplete();
                    return percentComplete == null ? 0.0 : percentComplete;
                }).average().orElse(0.0);
                throw new RuntimeException(String.format("(%d%%) This datadoc is currently saving and not ready to view yet", Math.round(averagePercentage)));
            }
            securityService.checkDatadocAccess(datadoc, DatadocActionAccessibility.GET_FILE);
            tableService.initSharedDatadocIfNeeded(datadoc, false, Auth.get().getUserId());
        } else if(file instanceof Upload) {
            uploadRepository.updateFile(file);
        }
        log.info("Retrieve file {} took {}", id, stopwatch.stop());
        return file;
    }

    @ApiOperation("Prepare upload by retrieving N resumable signed links to GCS")
    @RequestMapping(value = "/upload/prepare", method = RequestMethod.POST)
    public List<PrepareUploadResponse> prepareUpload(@RequestBody PrepareUploadRequest request) {
        return uploadService.prepareUpload(request);
    }

    @ApiOperation("Confirm upload has successfully finished")
    @RequestMapping(value = "/upload/confirm", method = RequestMethod.POST)
    public ConfirmUploadResponse confirmUpload(@RequestBody ConfirmUploadRequest request) {
        return uploadService.confirmUpload(request);
    }

    @GetMapping(value = "/get_file/{shareId}")
    public GetDatadocIdResponse getFile(@PathVariable UUID shareId) throws AuthenticationController.NotFoundException {
        Datadoc datadoc = uploadRepository.getDatadocByShareId(shareId);
        if(datadoc == null) {
            throw new AuthenticationController.NotFoundException();
        }
        return new GetDatadocIdResponse(datadoc.getId());
    }

    @ApiOperation("Refresh database tables")
    @RequestMapping(value = "/refresh_tables", method = RequestMethod.POST)
    public AbstractFile refreshDatabaseTables(@RequestBody RefreshTablesRequest getFileRequest) {
        Long id = uploadRepository.resolveFileIdByPath(getFileRequest.getPath());
        if (id == null) {
            throw new RuntimeException("File not found!");
        }
        return uploadService.refreshTables(id);
    }

    @ApiOperation("Get description of all files at provided location")
    @RequestMapping(value = "/list_files", method = RequestMethod.POST)
    public List<AbstractFile> getFiles(@RequestBody ListFilesRequest request) {
        Long id = uploadRepository.resolveFileIdByPath(request.getPath(), false);
        try {
            if(request.isSourcesOnly() || request.isFoldersOnly() || request.isDatadocsOnly()) {
                return uploadRepository.getFiles(Lists.newArrayList(), Auth.get().getUserId(), id,
                        request.getOffset(),
                        request.getLimit(),
                        request.getOrderBy(),
                        request.isWithSections(),
                        request.isWithDatadocs(),
                        request.isSourcesOnly(),
                        request.isFoldersOnly(),
                        request.isDatadocsOnly());
            } else {
                return Lists.newArrayList();
            }
        } catch (Exception e){ // todo remove after all order properties are implemented
            e.printStackTrace();
            return new ArrayList<>();
        }
    }

    @ApiOperation("Get description of all recent files")
    @RequestMapping(value = "/recent_files", method = RequestMethod.POST)
    public List<AbstractFile> getRecent(@RequestBody RecentFilesRequest listFilesRequest){
        return uploadRepository.getRecentFiles(Auth.get().getUserId(), listFilesRequest.getLimit());
    }

    @RequestMapping(value = "/create-from-storage", method = RequestMethod.POST)
    public File createFileFromStorage(@RequestBody SaveFileResponse request) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Long id = uploadRepository.resolveFileIdByPath(request.getPath());
        File file = uploadService.createFile(Auth.get().getUserId(), request.getDescriptorId(), id);
        log.info("Create file {} from storage took {}", file.getName(), stopwatch.stop());
        return file;
    }

    @ApiOperation(value = "Upload file", notes = "Accepts header <b>Datadocs-API-Arg</b>. Example: <br/><br/> {\"path\": \"/upload\"} <br/><br/><b>path</b> - upload path")
    @RequestMapping(value = "/upload", method = RequestMethod.POST)
    public SaveFileResponse uploadFile(HttpServletRequest request,
                           @RequestHeader(required = false, value = ApiController.API_ARG_NAME) String downloadRequestString) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        boolean isMultipart = ServletFileUpload.isMultipartContent(request);
        if(!isMultipart){
            throw new RuntimeException("File contents not found");
        }
        ServletFileUpload upload = new ServletFileUpload();
        FileItemIterator iterator = upload.getItemIterator(request);
        while(iterator.hasNext()){
            FileItemStream item = iterator.next();
            if(!item.isFormField() && "file".equals(item.getFieldName())) {
                if(DataFormat.isFileFormatAvailable(item.getName())) {
                    UploadFileRequest uploadFileRequest = JsonUtils.readValue(downloadRequestString, UploadFileRequest.class);
                    Long descriptorId = uploadService.saveFile(uploadFileRequest, item);
                    log.info("File uploading {} took {}", item.getName(), stopwatch.stop());
                    return new SaveFileResponse(uploadFileRequest.getPath(), descriptorId);
                } else {
                    throw new IllegalArgumentException("This file format is not acceptable. Acceptable formats: " + DataFormat.getAvailableFileFormats());
                }
            }
        }
        throw new RuntimeException("File contents not found");
    }

    @ApiOperation(value = "Download single file",
            notes = "Accepts header <b>Datadocs-API-Arg</b>. Example: <br/><br/> {\"path\": \"/download/Screen Shot 2015-12-11 at 10.43.14 AM.png\"} <br/><br/><b>path</b> - file path")
    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public void downloadFile(HttpServletResponse response,
                             @RequestParam(required = false) Long id,
                             @RequestHeader(required = false, value = ApiController.API_ARG_NAME) String downloadRequestString) {
        if (id == null) {
            if (downloadRequestString != null) {
                DownloadRequest downloadRequest = JsonUtils.readValue(downloadRequestString, DownloadRequest.class);
                id = uploadRepository.resolveFileIdByPath(downloadRequest.getPath());
            } else {
                throw new RuntimeException("Please provide download path through " + API_ARG_NAME + " header or internal file ID through request parameter");
            }
        }
        AbstractFile abstractFile = uploadRepository.getFile(id);
        if (abstractFile instanceof File) {
            File file = (File) abstractFile;
            if (file.getDescriptor() instanceof FileDescriptor) {
                FileDescriptor fileDescriptor = (FileDescriptor) file.getDescriptor();
                try (InputStream is = storageStrategy.get(fileDescriptor).getFile(fileDescriptor.getPath())) {
                    // copy it to response's OutputStream
                    doFileResponse(fileDescriptor.getContentType(), file.getName(), is, response);
                } catch (Exception e) {
                    throw new RuntimeException("Can't download file", e);
                }
            }
        } else {
            throw new RuntimeException("Only single file can be downloaded! For downloading many files or folders please refer to /archive/prepare");
        }
    }

    @ApiOperation("Prepare archive of many files and folders")
    @RequestMapping(value = "/archive/prepare", method = RequestMethod.POST)
    public Map<String, Object> prepareFileDownload(@RequestBody PrepareDownloadRequest prepareDownloadRequest) {

        if (prepareDownloadRequest.getPaths().isEmpty()) {
            throw new RuntimeException("No files specified for downloading");
        }
        List<Long> ids = prepareDownloadRequest.getPaths()
                .stream()
                .map(path -> uploadRepository.resolveFileIdByPath(path))
                .collect(Collectors.toList());

        try {
            FileZipRequest request = new FileZipRequest();
            request.setFileIds(ids);
            String requestId = taskManagementService.execute(Auth.get(), request);
            return ImmutableMap.of("requestId", requestId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ApiOperation("Download prepared archive")
    @RequestMapping(value = "/archive/download", method = RequestMethod.GET)
    public void downloadArchive(HttpServletResponse response, @RequestParam(required = false) String requestId) {
        TaskInfo taskInfo = tasksRepository.getTaskInfo(requestId);
        if (taskInfo == null) {
            throw new RuntimeException("Task does not exist");
        } else if (!taskInfo.isFinished()) {
            throw new RuntimeException("Task has not finished yet");
        } else if (taskInfo.getResult() == null) {
            throw new RuntimeException("Task result is empty");
        }
        FileZipResult result = (FileZipResult) taskInfo.getResult();
        FileStorage fileStorage = storageStrategy.get(result.getDescriptor());
        try (InputStream is = fileStorage.getFile(result.getDescriptor().getPath())) {
            doFileResponse(result.getDescriptor().getContentType(), result.getDescriptor().getOriginalFileName(), is, response);
        } catch (IOException e) {
            throw new RuntimeException("Can't download archive");
        } finally {
            fileStorage.removeFile(result.getDescriptor().getPath());
        }
    }

    @ApiOperation("Get pending archive downloads")
    @RequestMapping(value = "/archive/pending", method = RequestMethod.GET)
    public List<TaskInfo> getPendingDownloads() {
        return tasksRepository.getPendingTasksAsList(Sets.newHashSet(FileZipTask.class.getSimpleName()), Auth.get().getUserId());
    }

    @ApiOperation(value = "Create folder", notes = "Creates folder at given path with name equal to last path section")
    @RequestMapping(value = "/create_folder", method = RequestMethod.POST)
    public Folder createFolder(@RequestBody CreateFolderRequest createFolderRequest) {
        Long id = uploadRepository.resolveFileIdByPath(FilenameUtils.getPath(createFolderRequest.getPath()), true);
        return uploadRepository.createFolder(id, FilenameUtils.getName(createFolderRequest.getPath()));
    }

    @ApiOperation(value = "Move file or folder to another location", notes = "Moves file/folder to another location (if <b>toPath</b> starts with <b>id:</b> or <b>/</b>) otherwise renames file")
    @RequestMapping(value = "/move", method = RequestMethod.POST)
    public List<AbstractFile> moveFiles(@RequestBody MoveRequest moveRequest) {
        return uploadRepository.moveFiles(moveRequest);
    }

    @ApiOperation("Delete file or folder")
    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    public void deleteFile(@RequestBody DeleteRequest deleteRequest) {
        List<Long> ids = deleteRequest.getPaths()
                .stream()
                .map(path -> uploadRepository.resolveFileIdByPath(path))
                .collect(Collectors.toList());
        uploadService.removeFiles(ids);
    }

    @ApiOperation("Delete all files by source uuid")
    @RequestMapping(value = "/delete_file_by_path", method = RequestMethod.POST)
    public void deleteFile(@RequestBody String path) {
        Callable<Boolean> deleteTask = () -> {
            boolean deleted = uploadService.deleteFileByUuid(path);
            if(deleted) {
                log.info("File {} has been canceled and removed. ", path);
            }
            return deleted;
        };
        SilentThreadWithRetries thread = new SilentThreadWithRetries(true, deleteTask, 5000L, 30);
        thread.start();
    }

    @ApiOperation("Delete all sources by user id")
    @RequestMapping(value = "/delete_all_sources", method = RequestMethod.DELETE)
    public Boolean deleteSourcesFiles() {

        try {
            uploadRepository.deleteUploadsByUserId(Auth.get().getUserId());
            return true;
        } catch (Exception ex) {
            log.error("Failed to remove all sources, reason: '{}'.", ExceptionUtils.getRootCauseMessage(ex));
        }

        return false;
    }

    @ApiOperation("Get remote schema")
    @RequestMapping(value = "/get_remote_schema", method = RequestMethod.POST)
    public List<TableInfo> getRemoteSchema(@Valid @RequestBody ConnectionParams params, Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors.getAllErrors());
        }
        return dbService.getTables(params);
    }

    @ApiOperation("Test connection")
    @RequestMapping(value = "/test_connection", method = RequestMethod.POST)
    public Upload testConnection(@Valid @RequestBody TestConnectionRequest testConnectionRequest, Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors.getAllErrors());
        }
        Long id = uploadRepository.resolveFileIdByPath(testConnectionRequest.getPath());
        return uploadService.testConnectionOfRemoteDataSource(id, testConnectionRequest);
    }

    @ApiOperation("Create remote link")
    @RequestMapping(value = "/create_remote_link", method = RequestMethod.POST)
    public Upload createRemoteLink(@Valid @RequestBody CreateRemoteLinkRequest createRemoteLinkRequest, Errors errors) {
        if(createRemoteLinkRequest.getCancel()) {
            cancellationService.add(Auth.get().getUserId(), createRemoteLinkRequest);
            return null;
        }

        if (errors.hasErrors()) {
            throw new ValidationException(errors.getAllErrors());
        }
        Long id = uploadRepository.resolveFileIdByPath(createRemoteLinkRequest.getPath(), true);
        return uploadService.createRemoteLink(Auth.get().getUserId(), id, createRemoteLinkRequest);
    }

    @ApiOperation("Update remote link")
    @RequestMapping(value = "/update_remote_link", method = RequestMethod.POST)
    public Upload updateRemoteLink(@Valid @RequestBody UpdateRemoteLinkRequest updateRemoteLinkRequest, Errors errors) {
        if(updateRemoteLinkRequest.getCancel()) {
            cancellationService.add(Auth.get().getUserId(), updateRemoteLinkRequest);
            return null;
        }
        if (errors.hasErrors()) {
            throw new ValidationException(errors.getAllErrors());
        }


        Long id = uploadRepository.resolveFileIdByPath(updateRemoteLinkRequest.getPath());
        return uploadService.updateRemoteLink(Auth.get().getUserId(), id, updateRemoteLinkRequest);
    }

    @ApiOperation("Create query for remote link")
    @RequestMapping(value = "/create_remote_query", method = RequestMethod.POST)
    public Upload createQueryForRemoteLink(@RequestBody CreateRemoteQueryRequest createRemoteQueryRequest) {
        Long id = uploadRepository.resolveFileIdByPath(createRemoteQueryRequest.getRemoteLinkPath());
        return uploadService.addQueryToRemoteDataSource(Auth.get().getUserId(),
                                                        id, createRemoteQueryRequest.getName(),
                                                        createRemoteQueryRequest.getQuery());
    }

    @ApiOperation("Update query for remote link")
    @RequestMapping(value = "/update_remote_query", method = RequestMethod.POST)
    public Upload updateQueryForRemoteDataSource(@RequestBody UpdateRemoteQueryRequest updateRemoteQueryRequest) {
        Long id = uploadRepository.resolveFileIdByPath(updateRemoteQueryRequest.getPath());
        return uploadService.editQueryOfRemoteDataSource(id, updateRemoteQueryRequest);
    }

    @ApiOperation("Update settings")
    @RequestMapping(value = "/update_settings", method = RequestMethod.POST)
    public Upload updateSettings(@Valid @RequestBody UpdateSettingsRequest updateSettingsRequest, Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors.getAllErrors());
        }
        Long id = uploadRepository.resolveFileIdByPath(updateSettingsRequest.getPath());
        return uploadService.updateSettings(id, updateSettingsRequest);
    }

    @ApiOperation("Preview data")
    @RequestMapping(value = "/preview", method = RequestMethod.POST)
    public PreviewResponse previewData(@Valid @RequestBody PreviewRequest previewRequest, Errors errors) {
        if (errors.hasErrors()) {
            throw new ValidationException(errors.getAllErrors());
        }
        Long id = uploadRepository.resolveFileIdByPath(previewRequest.getPath());
        return uploadService.previewData(id, previewRequest);
    }

    @RequestMapping(value = "/get_attached", method = RequestMethod.POST)
    public Map<Long, Collection<Long>> getFilesAttachedToIndexes(@RequestBody List<Long> fileIds) {
        return uploadService.getFilesAttachedToDatadocs(Auth.get().getUserId(), fileIds);
    }

    @ApiOperation("Annotate file, folder, tab or remote link")
    @RequestMapping(value = "/annotate", method = RequestMethod.POST)
    public AbstractFile annotate(@RequestBody AnnotationRequest annotationRequest) {
        Long id = uploadRepository.resolveFileIdByPath(annotationRequest.getPath());
        return uploadService.annotate(id, annotationRequest.getAnnotation());
    }

    @ApiOperation("Close sticky note for a file")
    @RequestMapping(value = "/close_sticky_note", method = RequestMethod.POST)
    public void closeStickyNote(@RequestBody CloseStickyNoteRequest closeStickyNoteRequest){
        Long id = uploadRepository.resolveFileIdByPath(closeStickyNoteRequest.getPath());
        uploadService.closeStickyNote(id, closeStickyNoteRequest.getStickyNoteType());
    }

}
