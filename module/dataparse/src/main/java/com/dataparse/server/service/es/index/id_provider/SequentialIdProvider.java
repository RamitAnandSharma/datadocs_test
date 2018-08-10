package com.dataparse.server.service.es.index.id_provider;


import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides ID from custom sequence.
 *
 */
public class SequentialIdProvider implements IdProvider {

    private AtomicLong seq;

    public SequentialIdProvider(Long currentSeqValue) {
        this.seq = new AtomicLong(currentSeqValue == null ? 0 : currentSeqValue);
    }

    @Override
    public String getId(Map<String, Object> o) {
        return String.valueOf(seq.incrementAndGet());
    }
}
