package com.dataparse.server.coverage.utils.report;

import com.dataparse.server.coverage.utils.helpers.CallableTest;
import com.dataparse.server.coverage.utils.helpers.IsolatedContext;
import com.dataparse.server.service.engine.EngineSelectionStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.junit.AfterClass;
import org.junit.Rule;


@Slf4j
public abstract class ReportedTest extends IsolatedContext {

  @Rule
  public CustomErrorCollector collector = new CustomErrorCollector();

  final static private boolean shouldGenerateReport = true;
  final static private boolean shouldSaveToDatabase = true;
  final static private ReportData reportData = new ReportData();

  // specific params for every child
  protected abstract String getTestName();
  protected abstract String getTestKey();
  protected abstract String getTestParamsString();

  protected Boolean includeTestReport = true;

  // =================================================================================================================

  // # for default cases without file usage|upload|parsing
  protected void runStep(CallableTest test, String subTestName, String description) {
    SubTestResult subTestResult = new SubTestResult(getTestName(), getTestParamsString(), getTestKey(), subTestName, description);
    stepFunc(test, subTestResult);
  }

  // # for cases with file usage|upload|parsing
  protected void runStep(CallableTest test, String subTestName, String description, String file,
      EngineSelectionStrategy engineSelectionStrategy) {

    String fileName = FilenameUtils.getName(file);
    String fileExt = FilenameUtils.getExtension(file);
    String backendEngine = guessEngineTypeName(engineSelectionStrategy);

    SubTestResult subTestResult = new SubTestResult(getTestName(), getTestParamsString(), getTestKey(), subTestName, description,
        fileName, fileExt, backendEngine);

    stepFunc(test, subTestResult);
  }

  // =================================================================================================================

  private void stepFunc(CallableTest test, SubTestResult subTestResult) {
    try {
      test.call();
    } catch (Exception e) {
      failTest(subTestResult, e);
    } catch (AssertionError assertionError) {
      // asserting test and failing status + rethrowing for test stop
      failTest(subTestResult, assertionError);
      throw assertionError;
    } finally {
      finalizeTest(subTestResult);
    }
  }

  // rethrowing error + fixing assertion results for our test
  private void failTest(SubTestResult subTestResult, Throwable error) {
    error.printStackTrace();
    collector.addError(error);
    subTestResult.setDescription(error.getMessage());
    if (subTestResult.getDescription() == null) {
      subTestResult.setDescription(error.toString());
    }
    subTestResult.setSucceeded(false);
  }

  // fixing test time and remember tests results
  private void finalizeTest(SubTestResult subTestResult) {
    subTestResult.fixDuration();

    try {
      collector.verifyErrors();
    } catch (Throwable throwable) {

      String descriptionMessage = throwable.getMessage();
      if(descriptionMessage != null && descriptionMessage.length() > 260) {
        descriptionMessage = descriptionMessage.substring(0, 256) + "... ";
      }

      subTestResult.setDescription(descriptionMessage);
      subTestResult.setSucceeded(false);
    }

    if (includeTestReport) {
      reportData.addSubTestResult(subTestResult);
    }

    collector = new CustomErrorCollector();
  }

  // guess, which engine is selected now
  private String guessEngineTypeName(EngineSelectionStrategy engineSelectionStrategy) throws RuntimeException {
    switch (engineSelectionStrategy)
    {
    case ALWAYS_ES:
      return "ES";
    case ALWAYS_BQ:
      return "BQ";
    case DEPENDING_ON_DATASET_SIZE:
      return "DEPENDS ON FILE SIZE";
    default:
      throw new RuntimeException("Unknown engine selection strategy.");
    }
  }

  // # printing report and saving data before shutdown
  // # !we will extend other tests from this one, so we need to generate one general report for all tests (not overwriting it)
  @AfterClass
  public static void saveReport() {
    log.info("All report data has been collected. Starting saving processing data.");
    if (shouldGenerateReport) {
      log.info("Generating report data and saving it.");
      TestReportGenerator testReportGenerator = new TestReportGenerator();
      testReportGenerator.generateReport(reportData);
    }
    if (shouldSaveToDatabase) {
      log.info("Sending report data to database.");
      TestReportDatabaseSaver testReportDatabaseSaver = new TestReportDatabaseSaver();
      testReportDatabaseSaver.saveReportData(reportData);
    }
  }

}
