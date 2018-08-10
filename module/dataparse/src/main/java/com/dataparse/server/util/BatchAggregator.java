package com.dataparse.server.util;

import com.dataparse.server.service.es.index.IndexRow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BatchAggregator implements Aggregator{

    private int total;
    private List<IndexRow> cache;
    private int batchSize;
    private Consumer<List<IndexRow>> callback;

    public BatchAggregator(int batchSize, Consumer<List<IndexRow>> callback) {
        this.batchSize = batchSize;
        this.callback = callback;
        this.cache = new ArrayList<>(batchSize);
    }

    @Override
    public void push(IndexRow command) {
        cache.add(command);
        total++;
        if (batchSize > 0
                && cache.size() >= batchSize) {
            flush();
        }
    }

    public void flush() {
        if (!cache.isEmpty()) {
            callback.accept(new ArrayList<>(cache));
            cache.clear();
        }
    }

    @Override
    public int getTotal() {
        return total;
    }

    @Override
    public long getTotalTime() {
        return 0;
    }
}
