package com.dataparse.server.service.bigquery.sync;

import com.dataparse.server.service.tasks.*;
import lombok.*;

import java.util.*;
import java.util.concurrent.*;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class SyncResult extends IngestTaskResult {
    private String tableName = null;
    private String accountId = null;
    private double percentComplete = 0.;
    private long total = 0;
    private boolean success;

    public static SyncResult reduceFNonInterrupted(List<? extends Future<SyncResult>> results){
        boolean interrupted = Thread.interrupted();
        SyncResult result = reduceF(results);
        if(interrupted){
            Thread.currentThread().interrupt();
        }
        return result;
    }

    public static SyncResult reduceF(List<? extends Future<SyncResult>> results){
        return results.stream()
                .map(r -> {
                    try {
                        return r.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .reduce(SyncResult::reduce)
                .orElse(SyncResult.empty());
    }

    public static SyncResult reduce(List<SyncResult> results){
        return results.stream()
                .reduce(SyncResult::reduce)
                .orElse(SyncResult.empty());
    }

    public static SyncResult reduce(SyncResult r1, SyncResult r2){
        SyncResult tmp = new SyncResult();
        tmp.setSuccess(r1.isSuccess() && r2.isSuccess());
        tmp.setTotal(r1.getTotal() + r2.getTotal());
        tmp.setExecutionTime(r1.getExecutionTime() + r2.getExecutionTime());
        tmp.setAllRowsCount(r1.getAllRowsCount());
        tmp.addProcessionErrors(r1.getProcessionErrors());
        tmp.addProcessionErrors(r2.getProcessionErrors());
        return tmp;
    }

    public static SyncResult empty(){
        return new SyncResult();
    }

}
