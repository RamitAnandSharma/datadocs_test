package com.dataparse.server.service.tasks;

import com.dataparse.server.service.flow.ErrorValue;
import lombok.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class IngestTaskResult extends TaskResult {

    private static final Integer MAX_UNIQUE_ERRORS_COUNT = 20;

    @Getter
    @Setter(AccessLevel.NONE)
    private long processionErrorsCount = 0;

    @Getter
    @Setter(AccessLevel.NONE)
    private List<ErrorValue> processionErrors = new ArrayList<>();
    private long total = 0;
    private long allRowsCount = 0;
    private long executionTime = 0;

    public Long getSuccessItemsCount() {
        return total - processionErrorsCount;
    }

    public void addProcessionErrors(List<ErrorValue> errors) {
        processionErrorsCount += errors.size();
        List<ErrorValue> newErrors = new HashSet<>(errors).stream().limit(MAX_UNIQUE_ERRORS_COUNT).collect(Collectors.toList());
        List<ErrorValue> newCapturedErrors = new ArrayList<>();
        newCapturedErrors.addAll(processionErrors);
        newCapturedErrors.addAll(newErrors);
        int cutUntil = newCapturedErrors.size() > MAX_UNIQUE_ERRORS_COUNT ? MAX_UNIQUE_ERRORS_COUNT : newCapturedErrors.size();
        processionErrors = newCapturedErrors.subList(0, cutUntil);
    }
}
