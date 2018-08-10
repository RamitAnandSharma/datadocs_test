package com.dataparse.server.util.thread;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class ThreadUtils {
    public static <T> CompletableFuture<T> run(Supplier<T> supplier) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        Executors.newCachedThreadPool().submit(() -> {
            T result = null;
            try {
                result = supplier.get();
            } finally {
                completableFuture.complete(result);
            }
        });
        return completableFuture;
    }
}
