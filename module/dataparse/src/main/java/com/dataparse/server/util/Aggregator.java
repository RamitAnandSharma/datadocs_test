package com.dataparse.server.util;

import com.dataparse.server.service.es.index.IndexRow;

public interface Aggregator {

    void push(IndexRow command);

    void flush();

    int getTotal();

    long getTotalTime();

}
