package com.dataparse.server.coverage.utils.report;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ReportData {

  private boolean succeeded = true;
  private Map<String, TestResult> data = new LinkedHashMap<>();

  @Getter
  private DateTime reportDateTime = new DateTime(DateTimeZone.UTC);
  private Long testsTotal = 0L;
  private Long succeededTotal = 0L;
  private Long failedTotal = 0L;
  private Long durationTotal = 0L;

  // merging steps(sub-tests) into one test
  public void addSubTestResult(SubTestResult subTestResult) {
    // managing storage keys
    String testKey = subTestResult.getBelongsTo();
    if (!data.containsKey(testKey)) {
      data.put(testKey, new TestResult(subTestResult.getTestName(), testKey));
    }

    // recounting statistics
    durationTotal += subTestResult.getDuration();
    testsTotal ++;

    if(subTestResult.getSucceeded()) {
      succeededTotal ++;
    } else {
      succeeded = false;
      failedTotal ++;
    }

    // filling specific test with data
    data.get(testKey).addSubTestResult(subTestResult);
  }

  Set<String> getTestsKeys() {
    return data.keySet();
  }

  Map<String, TestResult> getTestsData() {
    return data;
  }

  // counting global report statistics
  HashMap<String, String> getReportStatistics() {
    HashMap<String, String> statistics = new HashMap<>();

    statistics.put("reportDate", reportDateTime.toString());
    statistics.put("testsTotal", testsTotal.toString());
    statistics.put("succeededTotal", succeededTotal.toString());
    statistics.put("failedTotal", failedTotal.toString());
    statistics.put("durationTotal", DurationFormatUtils.formatDuration(durationTotal, "HH:mm:ss"));

    return statistics;
  }

}
