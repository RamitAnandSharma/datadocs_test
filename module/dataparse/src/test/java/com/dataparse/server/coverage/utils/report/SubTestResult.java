package com.dataparse.server.coverage.utils.report;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.time.DurationFormatUtils;


@Data
@AllArgsConstructor
public class SubTestResult {

    private Boolean succeeded = true;
    private String testName = "TEST-NAME";
    private String belongsTo = "TEST-KEY";
    private String name = "SUB-TEST";
    private String status = "OK";
    private String description = "DESCRIPTION";

    private String testParams = "NONE";
    private String testedFileName = "NONE";
    private String testedFileExt = "NONE";
    private String testBackendEngine = "NONE";

    private Long duration = 0L;
    private Long startTime = 0L;
    private Long endTime = 0L;

    // # create subTestResult with manual managed duration
    public SubTestResult(String testName, String testParams, String belongsTo, String subTestName, String description) {
        this.belongsTo = belongsTo;
        this.testName = testName;
        this.testParams = testParams;
        this.name = subTestName;
        this.description = description;

        this.startTime = System.currentTimeMillis();
    }

    // # create subTestResult with manual managed duration
    public SubTestResult(String testName, String testParams, String belongsTo, String subTestName, String description,
                         String fileName, String fileExt, String backendEngine) {
        this.belongsTo = belongsTo;
        this.testName = testName;
        this.testParams = testParams;
        this.name = subTestName;
        this.description = description;
        this.testedFileName = fileName;
        this.testedFileExt = fileExt;
        this.testBackendEngine = backendEngine;

        this.startTime = System.currentTimeMillis();
    }

    // # fix test duration
    public void fixDuration() {
        if(this.startTime != 0) {
            this.endTime = System.currentTimeMillis();
            this.duration = this.endTime - this.startTime;
        }
    }

    public String getHumanizedDuration() {
        return DurationFormatUtils.formatDuration(duration, "HH:mm:ss");
    }

}