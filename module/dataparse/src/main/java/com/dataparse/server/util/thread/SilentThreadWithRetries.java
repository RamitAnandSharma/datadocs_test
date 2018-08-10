package com.dataparse.server.util.thread;

import com.github.rholder.retry.*;
import com.google.common.base.Predicates;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


@Slf4j
public class SilentThreadWithRetries extends Thread {

    public SilentThreadWithRetries(Boolean daemon, Callable<Boolean> callable) {
        this(daemon, callable, 10000L, Integer.MAX_VALUE);
    }

    public SilentThreadWithRetries(Boolean daemon, Callable<Boolean> callable, Long retriesTimeout, Integer retriesCount) {
        super(() -> {
            Retryer<Boolean> retry = RetryerBuilder.<Boolean>newBuilder()
                    .retryIfException()
                    .retryIfResult(Predicates.equalTo(false))
                    .withWaitStrategy(WaitStrategies.fixedWait(retriesTimeout, TimeUnit.MILLISECONDS))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(retriesCount))
                    .build();
            try {
                retry.call(callable);
            } catch (ExecutionException | RetryException e) {
                log.error("Failed to perform background task. ", e);
            }
        });
        this.setDaemon(daemon);
    }

}
