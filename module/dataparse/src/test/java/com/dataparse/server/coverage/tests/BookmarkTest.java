package com.dataparse.server.coverage.tests;

import static org.hamcrest.core.IsEqual.equalTo;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.util.LinkedMultiValueMap;

import com.dataparse.server.controllers.api.file.ListFilesRequest;
import com.dataparse.server.controllers.api.table.CreateTableBookmarkRequest;
import com.dataparse.server.coverage.utils.helpers.FileList;
import com.dataparse.server.coverage.utils.mockTesting.IntegrativeDatadocTest;
import com.dataparse.server.service.docs.DatadocStatistics;
import com.dataparse.server.service.engine.EngineSelectionStrategy;
import com.dataparse.server.service.schema.TableRepository;
import com.dataparse.server.service.schema.TableService;
import com.dataparse.server.service.upload.Upload;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;


@RunWith(Parameterized.class)
public class BookmarkTest extends IntegrativeDatadocTest {

  // =================================================================================================================
  // PARAMETERS
  // =================================================================================================================

  @ClassRule
  public static final SpringClassRule SCR = new SpringClassRule();
  @Rule
  public final SpringMethodRule springMethodRule = new SpringMethodRule();

  @Parameterized.Parameters
  public static List<AbstractMap.SimpleEntry<String, EngineSelectionStrategy>> getTestParams() {
    List<AbstractMap.SimpleEntry<String, EngineSelectionStrategy>> testParams = new ArrayList<>();
    List<String> filesForTest = FileList.getTestMockFiles(thisTest);

    for (String file : filesForTest) {
      testParams.add(new AbstractMap.SimpleEntry<>(file, EngineSelectionStrategy.ALWAYS_BQ));
      testParams.add(new AbstractMap.SimpleEntry<>(file, EngineSelectionStrategy.ALWAYS_ES));
    }

    return testParams;
  }

  // =================================================================================================================
  // Data collecting and initialization
  // =================================================================================================================

  final private String testName = "Bookmark Test";
  final static private String thisTest = "bookmarkTest";

  final private String fileName;
  final private EngineSelectionStrategy engineSelectionStrategy;

  final private String testKey;
  final private String testParams;

  @Autowired
  TableService tableService;
  @Autowired
  TableRepository tableRepository;

  public BookmarkTest(AbstractMap.SimpleEntry<String, EngineSelectionStrategy> testParams) {

    String fileName = testParams.getKey();
    EngineSelectionStrategy engineSelectionStrategy = testParams.getValue();

    this.includeTestReport = true;
    this.testParams = String.format("{file:%s, engine: %s}", fileName, engineSelectionStrategy);
    this.testKey = String.format("%s [%s, %s]", testName, fileName, engineSelectionStrategy);

    this.fileName = fileName;
    this.engineSelectionStrategy = engineSelectionStrategy;
  }

  // =================================================================================================================
  // Test entry
  // =================================================================================================================

  @Test
  public void bookmarkTest() {
    runStep(this::removeFileIfExists, "Remove previously uploaded results", "Removed if existed.", fileName, engineSelectionStrategy);
    runStep(this::uploadFile, "File upload", "Uploaded file.", fileName, engineSelectionStrategy);
    runStep(this::verifyFile, "Verify upload", "Verified file upload.", fileName, engineSelectionStrategy);
    runStep(this::ingestFile, "Ingest file", "Ingested file.");
    runStep(this::changeBookmarkEngineType, "Change backend engine", "Changed engine type.", fileName, engineSelectionStrategy);
    runStep(this::checkHeadersIngest, "Check headers usage", "First row is used as headers.", fileName, engineSelectionStrategy);

    // todo: complete this test
    runStep(this::testDatadocDefaultBookmark, "Verify default bookmarks", "Tested default bookmark.",
        fileName, engineSelectionStrategy);
    runStep(this::testBookmarksSourcesList, "Verify bookmarks sources", "Verified available bookmark sources.",
        fileName, engineSelectionStrategy);
    runStep(this::testBookMarksBasicOperations, "Test basic bookmarks operations",
        "Tested bookmarks creation and removal.", fileName, engineSelectionStrategy);
    runStep(this::testBookMarksAdvanced, "Basic bookmarks operations", "Tested bookmark renaming and moving.",
        fileName, engineSelectionStrategy);

    runStep(this::removeFile, "File removal", "Removed file. Deleted datadoc.", fileName, engineSelectionStrategy);
  }

  // =================================================================================================================
  // Tests
  // =================================================================================================================

  private void testDatadocDefaultBookmark() throws IOException{
    DatadocStatistics datadocStatistics = sendGet(String.format("/api/docs/%s/stats", datadoc.getId()), DatadocStatistics.class);

    // check if datadoc creation was valid
    collector.checkThat(fileName, equalTo(datadocStatistics.getDatadocName()));
    collector.checkThat(datadoc.getCreated(), equalTo(datadocStatistics.getCreated()));
    collector.checkThat(datadoc.getUpdated(), equalTo(datadocStatistics.getCreated()));
    collector.checkThat(1, equalTo(datadocStatistics.getBookmarks().size()));
    collector.checkThat(userId, equalTo(datadocStatistics.getCreatedByUserId()));
    collector.checkThat(fileName, equalTo(datadocStatistics.getBookmarks().get(0).getBookmarkName()));
    collector.checkThat(true, equalTo(datadocStatistics.getBookmarks().get(0).isCreatedAndLastSavedAreSameToHours()));
  }

  private void testBookmarksSourcesList() throws IOException {
    ListFilesRequest listFilesRequest = ListFilesRequest.builder().path("id:").limit(10).sourcesOnly(true).build();
    Upload[] uploads = sendPost("files/list_files", listFilesRequest, Upload[].class);

    // check sources
    collector.checkThat(1, equalTo(uploads.length));
    collector.checkThat(fileId, equalTo(uploads[0].getId()));
    collector.checkThat(fileName, equalTo(uploads[0].getName()));
    collector.checkThat(false, equalTo(uploads[0].isDeleted()));
  }

  private void testBookMarksBasicOperations() {
    CreateTableBookmarkRequest createBlankTableBookmarkRequest = new CreateTableBookmarkRequest();
    createBlankTableBookmarkRequest.setDatadocId(datadoc.getId());

    CreateTableBookmarkRequest createCopyTableBookmarkRequest = new CreateTableBookmarkRequest();
    createCopyTableBookmarkRequest.setDatadocId(datadoc.getId());
    createCopyTableBookmarkRequest.setBookmarkStateId(new BookmarkStateId(mainTabId, currentMainBookmarkStateId));

    LinkedMultiValueMap<String, String> query = new LinkedMultiValueMap<>();
    query.add("datadocId", datadoc.getId().toString());

    // first is default
    //TableBookmark secondBookmark = sendPost("/api/docs/bookmarks", createBlankTableBookmarkRequest, TableBookmark.class);
    //TableBookmark thirdBookmark = sendPost("/api/docs/bookmarks", createBlankTableBookmarkRequest, TableBookmark.class);
    // TableBookmark fisrtBookmarkCopy = sendPost("/api/docs/bookmarks", createCopyTableBookmarkRequest, TableBookmark.class);
    //TableBookmark[] tableBookmarks = sendGet("/api/docs/bookmarks/all", query, TableBookmark[].class);
  }

  private void testBookMarksAdvanced() {
  }

  // =================================================================================================================
  // Override abstract
  // =================================================================================================================

  @Override
  public String getFileName() {
    return fileName;
  }

  @Override
  public String getThisTestFoldersName() {
    return  thisTest;
  }

  @Override
  public String getTestName() {
    return testName;
  }

  @Override
  public String getTestKey() {
    return testKey;
  }

  @Override
  public String getTestParamsString() {
    return testParams;
  }

  @Override
  protected EngineSelectionStrategy getCurrentEngineSelectionStrategy() {
    return this.engineSelectionStrategy;
  }

}
