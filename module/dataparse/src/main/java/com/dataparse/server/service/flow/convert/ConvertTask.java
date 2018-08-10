package com.dataparse.server.service.flow.convert;

import avro.shaded.com.google.common.collect.*;
import com.dataparse.server.service.tasks.*;

import java.util.*;

public class ConvertTask extends AbstractTask<ConvertTaskRequest> {

    public enum ConvertTaskState implements TaskState {
        CONVERT
    }

    @Override
    public Map<TaskState, Runnable> getStates() {
        return ImmutableMap.of(ConvertTaskState.CONVERT, () -> {

        });
    }

    @Override
    public void cancel() {

    }
}
