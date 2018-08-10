package com.dataparse.server.service.es.index;

import com.dataparse.server.service.tasks.IngestTaskResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.concurrent.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexResult extends IngestTaskResult {
    private String indexName = null;
    private long waitTime = 0;
    private long transformTime = 0;
    private long readTime = 0;
    private double percentComplete = 0.;
    private boolean success = false;

    public static IndexResult reduceFNonInterrupted(List<? extends Future<IndexResult>> results){
        boolean interrupted = Thread.interrupted();
        IndexResult result = reduceF(results);
        if(interrupted){
            Thread.currentThread().interrupt();
        }
        return result;
    }

    public static IndexResult reduceF(List<? extends Future<IndexResult>> results){
        return results.stream()
                .filter(r -> !r.isCancelled())
                .map(r -> {
                    try {
                        return r.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .reduce(IndexResult::reduce)
                .orElse(IndexResult.empty());
    }

    public static IndexResult reduce(List<IndexResult> results){
        return results.stream()
                .reduce(IndexResult::reduce)
                .orElse(IndexResult.empty());
    }

    public static IndexResult reduce(IndexResult r1, IndexResult r2){
        IndexResult tmp = new IndexResult();
        tmp.setSuccess(r1.isSuccess() && r2.isSuccess());
        tmp.setTotal(r1.getTotal() + r2.getTotal());
        tmp.setExecutionTime(r1.getExecutionTime() + r2.getExecutionTime());
        tmp.setWaitTime(r1.getWaitTime() + r2.getWaitTime());
        tmp.setAllRowsCount(r1.getAllRowsCount());
        tmp.addProcessionErrors(r1.getProcessionErrors());
        tmp.addProcessionErrors(r2.getProcessionErrors());
        return tmp;
    }

    public static IndexResult empty(){
        return new IndexResult();
    }
}
