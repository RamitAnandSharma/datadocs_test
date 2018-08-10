package com.dataparse.server.coverage.tests;

import com.dataparse.server.controllers.api.table.PresetBookmarkStateRequest;
import com.dataparse.server.controllers.api.table.SearchIndexRequest;
import com.dataparse.server.controllers.api.table.SearchIndexResponse;
import com.dataparse.server.coverage.utils.helpers.FileList;
import com.dataparse.server.coverage.utils.mockTesting.DatadocFiltersTester;
import com.dataparse.server.coverage.utils.mockTesting.IntegrativeDatadocTest;
import com.dataparse.server.service.engine.EngineSelectionStrategy;
import com.dataparse.server.service.visualization.Tree;
import com.dataparse.server.service.visualization.bookmark_state.BookmarkStateId;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

import static org.hamcrest.CoreMatchers.equalTo;


@Slf4j
@RunWith(Parameterized.class)
public class QueryScenarioTest extends IntegrativeDatadocTest {

  // building parameters for test based on list of files and storage strategies
  @Parameterized.Parameters
  public static List<SimpleEntry<String, EngineSelectionStrategy>> getTestParams() {
    List<SimpleEntry<String, EngineSelectionStrategy>> testParams = new ArrayList<>();
    List<String> filesForTest = FileList.getTestMockFiles(THIS_TEST);

    for (String file : filesForTest) {
      testParams.add(new SimpleEntry<>(file, EngineSelectionStrategy.ALWAYS_ES));
      testParams.add(new SimpleEntry<>(file, EngineSelectionStrategy.ALWAYS_BQ));
    }

    return testParams;
  }

  // =================================================================================================================

  final static private Boolean COLLECT_MOCKS = false;
  final static private EngineSelectionStrategy COLLECTION_ENGINE = EngineSelectionStrategy.ALWAYS_ES;

  final static private String THIS_TEST = "queryScenarioTest";

  final private String testName = "Query Scenario Test";
  final private String testKey;
  final private String testParams;

  // test params
  final private String fileName;
  final private EngineSelectionStrategy engineSelectionStrategy;

  private DatadocFiltersTester datadocFilterTester = new DatadocFiltersTester();

  // each test based on file\engine
  public QueryScenarioTest(SimpleEntry<String, EngineSelectionStrategy> testParams) {

    String fileName = testParams.getKey();
    EngineSelectionStrategy engineSelectionStrategy = testParams.getValue();

    this.includeTestReport = true;
    this.testParams = String.format("{file:%s, engine: %s}", fileName, engineSelectionStrategy);
    this.testKey = String.format("%s [%s, %s]", testName, fileName, engineSelectionStrategy);

    this.fileName = fileName;
    this.engineSelectionStrategy = engineSelectionStrategy;
  }

  // =================================================================================================================

  @Test
  public void queryScenarioTest() {
    log.info(String.format("### Execute scenario for %s [%s] ###", fileName, engineSelectionStrategy) );

    runStep(this::removeFileIfExists, "Remove previously uploaded results", "Removed if existed.", fileName, engineSelectionStrategy);
    runStep(this::uploadFile, "Upload file", "Uploaded file to storage.", fileName, engineSelectionStrategy);
    runStep(this::verifyFile, "Verify file", "Verified file status after uploading.", fileName, engineSelectionStrategy);
    runStep(this::ingestFile, "Ingest file", "Ingested file for processing.", fileName, engineSelectionStrategy);
    runStep(this::changeBookmarkEngineType, "Change backend engine", "Changed engine type.", fileName, engineSelectionStrategy);
    runStep(this::checkHeadersIngest, "Check headers usage", "First row is used as headers.", fileName, engineSelectionStrategy);

    runStep(this::prepareDatadocResultsTester, "Prepare test results matcher", "Prepared results matcher", fileName, engineSelectionStrategy);
    runStep(this::query1, "Query[1] file", "Proceeded first query.", fileName, engineSelectionStrategy);
    runStep(this::query2, "Query[2] file", "Proceeded second query.", fileName, engineSelectionStrategy);
    runStep(this::query3, "Query[3] file", "Proceeded third query.", fileName, engineSelectionStrategy);

    runStep(this::removeFile, "Remove file", "Removed tested file.", fileName, engineSelectionStrategy);
  }

  // =================================================================================================================

  private void prepareDatadocResultsTester() throws InterruptedException {
    log.info("### Preparing datadoc filter tester... ###");

    this.datadocFilterTester.setup(mainTabId, mainBookmarkId, currentMainBookmarkStateId, filters, columns, getSessionCookies(), getUserId());
    await(5000L);
  }

  private void query1() throws IOException, InterruptedException {
    log.info("### Executing first query... ###");
    String step = "1";

    tabStateToDefault();

    datadocFilterTester.sendShowC1C2C3(false);
    datadocFilterTester.sendOrderByC2C3C1();

    SearchIndexResponse response = sendPost("visualization/search", datadocFilterTester.buildSearchRequest(null), SearchIndexResponse.class);
    if(COLLECT_MOCKS && COLLECTION_ENGINE.equals(getCurrentEngineSelectionStrategy())) {
      saveResultToMock(response, step);
    }
    checkQueriesResults(response, datadocFilterTester.getQueryAnswer(fileName, step));
  }

  private void query2() throws IOException, InterruptedException {
    log.info("### Executing second query...");
    String step = "2";

    tabStateToDefault();
    getPagePreview();

    datadocFilterTester.sendC1FilterSelected();
    datadocFilterTester.sendC3SearchField();
    datadocFilterTester.sendShowC1C2C3(false);
    datadocFilterTester.sendOrderByC2C3C1();

    SearchIndexResponse response = sendPost("visualization/search", datadocFilterTester.buildSearchRequest(null), SearchIndexResponse.class);
    if(COLLECT_MOCKS && COLLECTION_ENGINE.equals(getCurrentEngineSelectionStrategy())) {
      saveResultToMock(response, step);
    }
    checkQueriesResults(response, datadocFilterTester.getQueryAnswer(fileName, step));
  }

  private void query3() throws IOException, InterruptedException {
    log.info("### Executing third query... ###");
    String step = "3";

    tabStateToDefault();
    getPagePreview();

    datadocFilterTester.sendC1FilterSelected();
    datadocFilterTester.sendC3SearchField();
    datadocFilterTester.sendGroupByC1();
    datadocFilterTester.sendShowC1C2C3(true);
    datadocFilterTester.sendOrderByC2C3C1();

    SearchIndexResponse response = sendPost("visualization/search", datadocFilterTester.buildSearchRequest(null), SearchIndexResponse.class);
    if(COLLECT_MOCKS && COLLECTION_ENGINE.equals(getCurrentEngineSelectionStrategy())) {
      saveResultToMock(response, step);
    }
    checkQueriesResults(response, datadocFilterTester.getQueryAnswer(fileName, step));
  }

  // =================================================================================================================

  private void checkQueriesResults(SearchIndexResponse fromTest, SearchIndexResponse fromFile) {
    log.info("Checking query results...");

    List<Tree.Node<Map<String, Object>>> rowsFromTest = datadocFilterTester.getCleanRowsData(fromTest);
    List<Tree.Node<Map<String, Object>>> rowsFromFile = datadocFilterTester.getCleanRowsData(fromFile);

    if(rowsFromFile.size() != rowsFromTest.size()) {
      log.error("Failed checking results: length mismatch (expected: {}, found: {})", rowsFromFile.size(),
          rowsFromTest.size());
      throw new RuntimeException("Cannot match query results - length of real and expected results differ...");
    }

    // check exact match each row (hard to look up for error in a single row if we get whole errors table)
    for(int idx = 0; idx < rowsFromTest.size(); idx ++) {
      try {
        checkRowDataEquality(rowsFromFile.get(idx).getData(), rowsFromTest.get(idx).getData(), idx + 1);
      } catch (RuntimeException ex) {
        collector.addError(ex);
      }
    }

    log.info("Finished checking results...");
  }

  private void checkRowDataEquality(Map<String, Object> origin, Map<String, Object> cmp, int row) {

    // row cells count equality check
    if(origin.keySet().size() != cmp.keySet().size()) {
      String errMessage = String.format("Cannot compare row [%s] data, different cells count: " +
          "expected [%s], but got [%s] \n", row, origin, cmp);
      throw new RuntimeException(errMessage);
    }

    // check each cell
    for(String key : origin.keySet()) {

      Object originValue = origin.get(key);
      Object cmpValue = cmp.get(key);

      // null values are not correctly checked by collector | but if they are equal and ok --> continue
      if(originValue == null && cmpValue == null) {
        continue;
      } else if(originValue == null || cmpValue == null) {
        String errorMessage = String.format("Values mismatch in row [%s]: expected [%s], but got [%s]",
            row, originValue, cmpValue);
        collector.addError(new RuntimeException(errorMessage));
        log.error(errorMessage);
        continue;
      }

      // check types of cell data
      if(!haveSameSuperclass(origin.getClass(), cmp.getClass())) {
        String errMessage = String.format("Types mismatch in row [%s]: expected [%s], but got [%s] \n",
            row, originValue.getClass(), cmpValue.getClass());
        throw new RuntimeException(errMessage);
      }

      // check data equality
      // convert all double formats to the same one [scientific like 1.6362136E-8 3.235235E7
      if(haveSameSuperclass(originValue.getClass(), Double.class)) {

        DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
        df.setMaximumFractionDigits(340); // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS

        collector.checkThat(df.format(cmpValue), equalTo(df.format(originValue)));
        if (!df.format(cmpValue).equals(df.format(originValue))) {
          log.error("Isn't equal " + df.format(cmpValue) + " to " + df.format(originValue));
        }
      } else {
        if(cmpValue.getClass() == String.class) {
          cmpValue = ((String) cmpValue).trim();
          originValue = ((String) cmpValue).trim();
        }

        if (!cmpValue.equals(originValue)) {
          log.error("Isn't equal " + cmpValue + " to " + originValue);
        }

        collector.checkThat(cmpValue, equalTo(originValue));
      }

    }

  }

  private void getPagePreview() throws IOException {

    log.info("Getting page rows preview...");

    SearchIndexRequest request = datadocFilterTester.buildSearchRequest(null);
    SearchIndexResponse response = sendPost("visualization/search", request, SearchIndexResponse.class);

    List<Tree.Node<Map<String, Object>>> rows = response.getData().getChildren();
    Map<String, Object> row;

    if(rows.size() == 0) {
      row = null;
    } else {
      row = rows.get(0).getData();
    }

    datadocFilterTester.setRowPreview(row);
    log.info(String.format("Got page search preview... [%s]", mapper.writeValueAsString(row)) );
  }

  private void saveResultToMock(SearchIndexResponse response, String queryId) {

    String filePath;
    String fileName = String.format(datadocFilterTester.getTestsAnswersDirectoryTemplate(), this.getFileName(), queryId);
    URL fileUrl = this.getClass().getClassLoader().getResource(fileName);

    if(fileUrl != null) {
      filePath = fileUrl.getPath();
    } else {
      // create file
      filePath = "/home/neloreck/temp.txt";
    }

    try {
      String content = mapper.writeValueAsString(response);

      FileWriter fw = new FileWriter(filePath);
      BufferedWriter bw = new BufferedWriter(fw);

      bw.write(content);
      bw.close();
      fw.close();

      log.info("Successfully saved mock result...");
    } catch (IOException e) {
      collector.addError(e);
    }
  }

  // should be temporary [?]
  // bad type-checking for comparison of LONG(BQ) and DOUBLE(ES) timestamps + other similar types
  private static boolean haveSameSuperclass(Class<?> left, Class<?> right) {
    return right.getGenericSuperclass().equals(left.getGenericSuperclass());
  }

  // reset state to default (filters, sorting etc)
  private void tabStateToDefault() throws IOException, InterruptedException {
    log.info("Cleaning up bookmark state...");
    BookmarkStateId stateId = new BookmarkStateId(mainBookmarkId, null);

    PresetBookmarkStateRequest presetBookmarkStateRequest = new PresetBookmarkStateRequest();
    presetBookmarkStateRequest.setToCleanState(true);
    presetBookmarkStateRequest.setBookmarkStateId(stateId);

    await(1000L);
    sendPost("docs/bookmarks/preset_default", presetBookmarkStateRequest, null);

    // reset selected local filters
    datadocFilterTester.resetFilters();

    datadocFilterTester.sendToggleC1C2C3();
    datadocFilterTester.sendHideNulls();

    log.info("Cleaned up bookmark state to default.");
  }

  // =================================================================================================================

  @Override
  public String getFileName() {
    return fileName;
  }

  @Override
  public String getThisTestFoldersName() {
    return THIS_TEST;
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

