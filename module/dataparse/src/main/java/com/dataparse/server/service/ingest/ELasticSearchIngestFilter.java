package com.dataparse.server.service.ingest;

import org.springframework.stereotype.Service;

@Service
public class ELasticSearchIngestFilter extends IngestFilter {

    @Override
    protected Integer getValueLengthThreshold() {
        return 32766;
    }

    @Override
    protected Integer getArrayLengthThreshold() {
        return 1000;
    }
}
