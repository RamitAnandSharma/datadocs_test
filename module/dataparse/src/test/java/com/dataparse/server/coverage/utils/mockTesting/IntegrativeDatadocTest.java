package com.dataparse.server.coverage.utils.mockTesting;

import com.dataparse.server.controllers.api.file.*;
import com.dataparse.server.controllers.api.flow.ExecutionRequest;
import com.dataparse.server.controllers.api.table.CreateDatadocRequest;
import com.dataparse.server.controllers.exception.ResourceAlreadyExists;
import com.dataparse.server.coverage.utils.helpers.FileList;
import com.dataparse.server.coverage.utils.helpers.TestGDriveFileStorage;
import com.dataparse.server.coverage.utils.session.IsolatedContextWithSession;
import com.dataparse.server.coverage.utils.socket.SocketManager;
import com.dataparse.server.service.docs.Datadoc;
import com.dataparse.server.service.engine.EngineSelectionStrategy;
import com.dataparse.server.service.engine.EngineType;
import com.dataparse.server.service.files.File;
import com.dataparse.server.service.files.Folder;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.schema.TableBookmark;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import com.dataparse.server.service.visualization.bookmark_state.event.ingest.EngineSelectionStrategyChangeEvent;
import com.dataparse.server.service.visualization.bookmark_state.filter.Filter;
import com.dataparse.server.service.visualization.bookmark_state.state.BookmarkState;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;
import static org.hamcrest.core.IsEqual.equalTo;


@Slf4j
public abstract class IntegrativeDatadocTest extends IsolatedContextWithSession {

    private static final String DATADOCS_FOLDER_PATH = "id:/test_results";
    private static final String DATADOCS_FOLDER_NAME = "test_results";

    private TestGDriveFileStorage testGDriveFileStorage;
    private String GDUploadKey;

    private String fileUploadSessionId;
    private long fileSize;
    private String fileHash;

    protected Long fileId;
    protected Long parentFolderId;
    protected Datadoc datadoc;
    protected List<ColumnInfo> columns;

    protected BookmarkState mainBookmarkState;
    protected UUID currentMainBookmarkStateId;
    protected Long mainBookmarkId;
    protected Long mainTabId;
    protected List<Filter> filters;


    public IntegrativeDatadocTest() {
        try {
            testGDriveFileStorage = new TestGDriveFileStorage();
            testGDriveFileStorage.init();
        } catch (Exception error) {
            collector.addError(error);
        }
    }

    abstract protected String getThisTestFoldersName();

    abstract protected String getFileName();

    abstract protected EngineSelectionStrategy getCurrentEngineSelectionStrategy();

    // =================================================================================================================

    protected void removeFileIfExists() throws IOException, InterruptedException {
        log.info("### Checking file existence ###");
        this.ensureTestsFolderExistence();

        ListFilesRequest listFilesInResFolderRequest = ListFilesRequest.builder()
                .path(IntegrativeDatadocTest.DATADOCS_FOLDER_PATH).sourcesOnly(true).offset(0).limit(50)
                .build();

        ListFilesRequest listFilesInRootFolderRequest = ListFilesRequest.builder()
                .path("id:").sourcesOnly(true).offset(0).limit(50)
                .build();

        Upload[] resultUploads = sendPost("files/list_files", listFilesInResFolderRequest, Upload[].class);
        Upload[] rootUploads = sendPost("files/list_files", listFilesInRootFolderRequest, Upload[].class);
        Upload[] srcFiles = ArrayUtils.addAll(rootUploads, resultUploads);

        for(Upload srcFile: srcFiles) {
            if(srcFile.getName().equals(getFileName())) {
                log.info("Found previously uploaded file...");
                sendRemoveFilesRequest(asList("id:" + srcFile.getId()));
            }
        }
    }

    protected void uploadFile() throws IOException {
        log.info("### Executing file upload... ###");

        PrepareUploadRequest prepareUploadRequest = PrepareUploadRequest.builder().count(1).build();
        PrepareUploadResponse[] prepareUploadResponse = sendPost("files/upload/prepare", prepareUploadRequest,
                PrepareUploadResponse[].class);
        fileUploadSessionId = prepareUploadResponse[0].getSessionId();

        log.info("Uploading...");
        GDUploadKey = testGDriveFileStorage.saveFile(FileList.getResourceFileStream(getThisTestFoldersName(), getFileName()), fileUploadSessionId);
        log.info("Uploaded. Object size = {}", testGDriveFileStorage.getFileSize(GDUploadKey));

        log.info("Downloading...");

        try {
            this.fileHash = md5Hex(testGDriveFileStorage.getFile(GDUploadKey));
            this.fileSize = testGDriveFileStorage.getFile(GDUploadKey).skip(Long.MAX_VALUE);
        } catch (Throwable throwable) {
            Assert.fail("Failed downloading file from GDrive: " + throwable.getMessage());
        }

        log.info("Loaded from storageGD: {}", fileSize);
    }

    protected void verifyFile() throws URISyntaxException, IOException {
        log.info("### Verifying file... ###");

        ConfirmUploadRequest confirmUploadRequest = ConfirmUploadRequest.builder()
                .contentType(FileList.getFileContentType(getThisTestFoldersName(), getFileName()))
                .fileId(GDUploadKey)
                .fileName(getFileName())
                .fileSize(fileSize)
                .hash(fileHash)
                .path(IntegrativeDatadocTest.DATADOCS_FOLDER_PATH)
                .sessionId(fileUploadSessionId)
                .build();

        try {
            ConfirmUploadResponse response = sendPost("files/upload/confirm", confirmUploadRequest, ConfirmUploadResponse.class);
            this.fileId = response.getFile().getId();
        } catch (HttpClientErrorException e) {
            // handle existed file
            if (e.getStatusCode() == HttpStatus.CONFLICT) {
                ResourceAlreadyExists response = mapper.readValue(e.getResponseBodyAsString(), ResourceAlreadyExists.class);
                this.fileId = response.getResourceId();
            }
        }

        Assert.assertNotNull(this.fileId);

        log.info("Finished verification for file {}.", this.fileId);
    }

    protected void ingestFile() throws IOException, InterruptedException {
        log.info("### Ingesting... ###");

        DateFormat formatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);
        String newDatadocName = String.format("%s | %s | %s | [%s]",
                formatter.format(new Date()), getTestName(), getFileName(), this.getCurrentEngineType());

        CreateDatadocRequest createDatadocRequest = CreateDatadocRequest.builder()
                .autoIngest(true).embedded(true).preSave(false)
                .name(newDatadocName)
                .parentId(null).preProcessionTime(7316L)
                .sourcePath("id:" + this.fileId)
                .parentId(this.parentFolderId)
                .build();

        this.datadoc = sendPost("docs", createDatadocRequest, Datadoc.class);
        Assert.assertNotNull(this.datadoc);
        waitForTaskExecutionRestfully(this.datadoc.getGathererTask(), 3000L);
        fetchBookmarkData(this.datadoc.getId());
    }

    protected void changeBookmarkEngineType() throws InterruptedException, IOException {
        log.info("### Changing bookmark[{}] engine type to {}. ###", this.mainBookmarkId, getCurrentEngineType().name());

        Assert.assertNotNull(this.mainBookmarkId);
        await(3000L);

        EngineSelectionStrategyChangeEvent engineSelectionStrategyChangeEvent = new EngineSelectionStrategyChangeEvent();
        engineSelectionStrategyChangeEvent.setEngineSelectionStrategy(getCurrentEngineSelectionStrategy());
        engineSelectionStrategyChangeEvent.setTabId(mainBookmarkId);
        engineSelectionStrategyChangeEvent.setStateId(currentMainBookmarkStateId);
        engineSelectionStrategyChangeEvent.setUser(userId);
        engineSelectionStrategyChangeEvent.setId(UUID.randomUUID().toString());
        engineSelectionStrategyChangeEvent.setInstanceId(UUID.randomUUID().toString());

        SocketManager socketManager = new SocketManager(getSessionCookies(), getUserId());

        socketManager.sendEventAndAwait(engineSelectionStrategyChangeEvent, mainBookmarkId, currentMainBookmarkStateId);
        await(2000L);

        ExecutionRequest executionRequest = ExecutionRequest.builder()
                .bookmarkId(this.mainBookmarkId)
                .engineType(getCurrentEngineType())
                .build();

        String taskId = sendPost("/flow/execute", executionRequest, String.class);
        waitForTaskExecutionRestfully(taskId, 3000L);

        log.info("Bookmark[{}] engine type has been changed.", this.mainBookmarkId);
    }

    protected void checkHeadersIngest() throws IOException {
        log.info("### Checking if headers were ingested... ###");
        Assert.assertNotNull(this.fileId);

        GetFileRequest getFileRequest = GetFileRequest.builder()
                .path("id:" + this.fileId)
                .relatedDatadocs(false)
                .sections(false)
                .build();

        File file = sendPost("files/get_file", getFileRequest, File.class);
        DataFormat fileFormat = file.getDescriptor().getFormat();
        Boolean usesHeaders;

        // extend there for required formats with headers
        // json, for example, always uses fields names etc
        switch (fileFormat) {

            case CSV:
                usesHeaders = ((CsvFileDescriptor)file.getDescriptor()).getSettings().getUseHeaders();
                if(usesHeaders != null) {
                    collector.checkThat(usesHeaders, equalTo(true));
                }
                break;

            case XLSX:
            case XLSX_SHEET:
                usesHeaders = ((XlsFileDescriptor)file.getDescriptor()).getSettings().getUseHeaders();
                if(usesHeaders != null) {
                    collector.checkThat(usesHeaders, equalTo(true));
                }
                break;

            default:
                log.info("Headers are always used by default for this format ({}).", file.getDescriptor().getFormat());
                break;
        }
    }

    protected void removeFile() throws IOException, InterruptedException {
        log.info("### Removing the file... ###");
        await(3000L);

        String fileToRemove = "id:" + this.fileId;
        sendRemoveFilesRequest(asList(fileToRemove));

        log.info("Finished test...");
        log.info("===================================================================================================");
    }

    // =================================================================================================================

    private void ensureTestsFolderExistence() throws IOException {
        ListFilesRequest listFilesRequest = ListFilesRequest.builder()
                .path("id:").foldersOnly(true).offset(0).limit(50)
                .build();

        // check if folder exists
        Folder[] folders = sendPost("files/list_files", listFilesRequest, Folder[].class);
        for (Folder folder : folders) {
            if (folder.getName().equals(IntegrativeDatadocTest.DATADOCS_FOLDER_NAME)) {
                log.info("Using existent folder for datadocs.");
                this.parentFolderId = folder.getId();
                return;
            }
        }

        // folder creation is required
        log.info("Creating new folder for datadocs.");

        CreateFolderRequest createFolderRequest = new CreateFolderRequest();
        createFolderRequest.setPath("/" + IntegrativeDatadocTest.DATADOCS_FOLDER_NAME);
        Folder folder = sendPost("files/create_folder", createFolderRequest, Folder.class);

        this.parentFolderId = folder.getId();
    }

    private void sendRemoveFilesRequest(List<String> files) throws IOException, InterruptedException {
        log.info("Sending remove file request... [{}]", files);
        DeleteRequest deleteRequest = DeleteRequest.builder().paths(files).build();
        sendPost("files/delete", deleteRequest, null);

        // await for processing before next test
        await(7000L);
    }

    private void fetchBookmarkData(Long datadocId) throws IOException {
        LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("datadocId", datadocId.toString());
        params.add("fetchState", "true");

        TableBookmark[] bookmarks = sendGet("/api/docs/bookmarks/all", params, TableBookmark[].class);
        TableBookmark bookmark = bookmarks[0];

        Assert.assertNotNull(bookmark);

        this.filters = bookmark.getState().getQueryParams().getFilters();
        this.mainBookmarkState = bookmark.getState();
        this.columns = bookmark.getTableSchema().getDescriptor().getColumns();
        this.currentMainBookmarkStateId = bookmark.getCurrentState();
        this.mainBookmarkId = bookmark.getId();
        this.mainTabId = bookmark.getTableSchema().getId();

        log.info("Datadoc example values: {} ;", this.columns.stream()
                .map(item -> new Pair<>(item.getAlias(), item.getExampleValue()))
                .collect(Collectors.toList()));
    }

    private EngineType getCurrentEngineType() throws RuntimeException {
        switch (this.getCurrentEngineSelectionStrategy()) {

            case ALWAYS_BQ:
                return EngineType.BIGQUERY;
            case ALWAYS_ES:
                return EngineType.ES;

            case DEPENDING_ON_DATASET_SIZE:
            default:
                throw new RuntimeException("Engine type must be provided directly for this test.");
        }
    }

}
