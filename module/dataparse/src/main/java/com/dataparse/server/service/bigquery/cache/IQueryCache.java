package com.dataparse.server.service.bigquery.cache;


import com.dataparse.server.service.bigquery.cache.serialization.*;

public interface IQueryCache<T> {

    void put(T key, BigQueryResult response);

    BigQueryResult get(T key);

    void evict(String externalId);
}
