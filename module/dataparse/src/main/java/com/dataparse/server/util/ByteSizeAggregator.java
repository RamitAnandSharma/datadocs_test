package com.dataparse.server.util;

import com.dataparse.server.service.es.index.IndexRow;
import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
public class ByteSizeAggregator implements Aggregator {

    private long totalBatchTime = 0;
    private int total;
    private long totalTime = 0;
    private List<IndexRow> cache;
    private Stopwatch timer;
    private long batchByteSize;
    private long maxBatchByteSize;
    private Consumer<List<IndexRow>> callback;

    public ByteSizeAggregator(long maxBatchByteSize, Consumer<List<IndexRow>> callback) {
        this.maxBatchByteSize = maxBatchByteSize;
        this.callback = callback;
        this.cache = new ArrayList<>(1000);
        this.timer = Stopwatch.createStarted();
    }

    @Override
    public void push(IndexRow command) {
        cache.add(command);
        total++;
        long start = System.nanoTime();
        String serialized = JsonUtils.writeValue(command);
        long end = System.nanoTime();
        totalBatchTime += end - start;
        batchByteSize += serialized.getBytes(Charsets.UTF_8).length;
        if (batchByteSize >= maxBatchByteSize) {
            flush();
        }
    }

    public void flush() {
        if (!cache.isEmpty()) {
            long elapsed = timer.stop().elapsed(TimeUnit.MILLISECONDS);
            totalTime += elapsed;
            log.info("Read batch of {} bytes in {}, size calculations: " + ((double) totalBatchTime / 1E6 ) + "", batchByteSize, elapsed);
            totalBatchTime = 0;
            callback.accept(new ArrayList<>(cache));
            cache.clear();
            batchByteSize = 0;
            timer = Stopwatch.createStarted();
        }
    }

    @Override
    public int getTotal() {
        return total;
    }

    @Override
    public long getTotalTime() { return totalTime; }
}
