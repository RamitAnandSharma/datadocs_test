package com.dataparse.server.util.thread;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Delegate;

import java.util.concurrent.Future;

@Data
@AllArgsConstructor
public class ErrorTaskExecution<T> {
    @Delegate
    private Future<T> failedTask;
    private Exception exception;
}
