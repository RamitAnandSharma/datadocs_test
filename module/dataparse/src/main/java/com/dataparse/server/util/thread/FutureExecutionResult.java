package com.dataparse.server.util.thread;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FutureExecutionResult<T> {
    private T result;
    private ErrorTaskExecution<T> error;

    public Exception getException() {
        if(this.isError()) {
            return this.error.getException();
        }
        return null;
    }

    public FutureExecutionResult(T result) {
        this.result = result;
    }

    public FutureExecutionResult(ErrorTaskExecution<T> error) {
        this.error = error;
    }

    public Boolean isSuccess() {
        return result != null;
    }

    public Boolean isError() {
        return error != null;
    }
}
