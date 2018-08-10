package com.dataparse.server.service.tasks;

import lombok.*;

import java.util.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskStatistics
{
    private Date requestReceivedTime = new Date();
    private Date requestCompleteTime;
    private long duration;
    private Map<String, Integer> stateRetries = new HashMap<>();

    // exception could be strict
    private List<? extends ExecutionException.Error> errors;

    // or general
    private String lastErrorMessage;
    private String lastErrorStackTrace;
    private String lastErrorRootCauseMessage;
}
