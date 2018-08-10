package com.dataparse.server.util;


import com.dataparse.server.util.thread.ErrorTaskExecution;
import com.dataparse.server.util.thread.FutureExecutionResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;


public class ConcurrentUtils {

    public static <T> Future<Boolean> invokeAfterFinish(Future<T> task, Consumer<T> consumer) {
        return invokeAfterFinish(task, consumer, 300);
    }

    public static void setTimeout(Runnable runnable, int delay){
        new Thread(() -> {
            try {
                Thread.sleep(delay);
                runnable.run();
            }
            catch (Exception e){
                throw new RuntimeException(e);
            }
        }).start();
    }


    public static <T> FutureExecutionResult<T> waitUntilFinished(Future<T> task) {
        while (!task.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return new FutureExecutionResult<>(task.get());
        } catch (InterruptedException | ExecutionException e) {
            return new FutureExecutionResult<>(new ErrorTaskExecution<>(task, e));
        }
    }

    public static void main(String[] args) {
        boolean[] finishedMask = new boolean[1];
        System.out.println(finishedMask[0]);
    }

    public static <T> List<FutureExecutionResult<T>> waitUntilFinished(Future<T> ... tasks) {
        Integer finishedCount = 0;
        List<FutureExecutionResult<T>> result = new ArrayList<>();
        boolean[] finishedMask = new boolean[tasks.length];

        while (finishedCount != tasks.length) {
            for (int i = 0; i < tasks.length; i++) {
                if(!finishedMask[i] && tasks[i].isDone()) {
                    finishedCount++;
                    try {
                        result.set(i, new FutureExecutionResult<>(tasks[i].get()));
                    } catch (ExecutionException | InterruptedException e) {
                        result.set(i, new FutureExecutionResult<>(new ErrorTaskExecution<>(tasks[i], e)));
                    } finally {
                        finishedMask[i] = true;
                    }
                }
            }
        }
        return result;
    }

    public static <T> Future<Boolean> invokeAfterFinish(Future<T> task, Consumer<T> consumer, Integer checkDelay) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor.submit(() -> {

            while (!task.isDone()) {
                try {
                    Thread.sleep(checkDelay);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Task has been interrupted.", e);
                }
            }
            T t = null;
            try {
                t = task.get();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException("Task execution has been interrupted. ");
            }
            consumer.accept(t);
            return true;
        });
    }
}
