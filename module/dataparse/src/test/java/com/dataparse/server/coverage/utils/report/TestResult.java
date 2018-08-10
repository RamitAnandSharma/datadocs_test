package com.dataparse.server.coverage.utils.report;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.util.ArrayList;
import java.util.List;


@Slf4j
@Getter
class TestResult {

    private boolean succeeded = true;
    private String thisTestKey = "THIS_TEST";
    private String name = "DEFAULT";
    private String testParams = "{}";

    private Long subTestsTotal = 0L;
    private Long succeededTotal = 0L;
    private Long failedTotal = 0L;
    private Long durationTotal = 0L;

    private List<SubTestResult> subTests = new ArrayList<>();

    TestResult(String name, String thisTestKey) {
        this.thisTestKey = thisTestKey;
        this.name = name;
    }

    void addSubTestResult(SubTestResult subTestResult) {
        this.subTests.add(subTestResult);
        this.durationTotal += subTestResult.getDuration();
        this.subTestsTotal ++;

        if(subTestResult.getSucceeded()) {
            this.succeededTotal ++;
        } else {
            this.succeeded = false;
            this.failedTotal ++;
        }
    }

    public String getHumanizedDuration() {
        return DurationFormatUtils.formatDuration(durationTotal, "HH:mm:ss");
    }

}
