package com.dataparse.server.service.bigquery.cache;

import com.dataparse.server.service.bigquery.cache.serialization.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * This cache provides an ability to lookup similar queries cached in Hazelcast by query key.
 * Similar query MUST return same results or results' superset (in terms of columns).
 * This cache is backed by a concurrent map which stores deque of QueryKey by Bookmark IDs.
 * Cache implementation is thread safe.
 */
@Service
public class IntelliCache implements IQueryCache<QueryKey> {

    private final static int MAX_QUERY_CACHE_SIZE = 100;

    @Autowired
    private HazelcastQueryCache queryCache;

    private final ConcurrentHashMap<String, Deque<QueryKey>> cache = new ConcurrentHashMap<>();

    /**
     * Searches for superset query key in a cache (linear-time complexity).
     * @param key Query key
     * @return Superset query key.
     */
    private QueryKey tryFindSupersetKey(QueryKey key){
        if(key == null || !key.isCacheable()){
            return null;
        }
        QueryKey superSetQuery = null;
        Deque<QueryKey> list = cache.get(key.getExternalId());
        if(list != null) {
            for (QueryKey cached : list) {
                if (key.isSubSetOf(cached)) {
                    superSetQuery = cached;
                    break;
                }
            }
        }
        return superSetQuery;
    }

    @Override
    public void put(final QueryKey key, final BigQueryResult response) {
        QueryKey superSetKey = tryFindSupersetKey(key);
        if(superSetKey == null) {
            queryCache.put(key.getHash(), response);
            Deque<QueryKey> list = cache.get(key.getExternalId());
            if(list == null){
                list = new LinkedBlockingDeque<>(MAX_QUERY_CACHE_SIZE);
                cache.put(key.getExternalId(), list);
            }
            if(key.isCacheable()) {
                if(list.size() >= MAX_QUERY_CACHE_SIZE){
                    list.removeFirst();
                }
                list.add(key);
            }
        }
    }

    @Override
    public BigQueryResult get(final QueryKey key) {
        BigQueryResult result = queryCache.get(key.getHash());
        if(result == null){
            QueryKey supersetKey = tryFindSupersetKey(key);
            if(supersetKey != null){
                result = queryCache.get(supersetKey.getHash());
                if(result == null){
                    Deque<QueryKey> list = cache.get(supersetKey.getExternalId());
                    if(list != null){
                        list.remove(supersetKey);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void evict(final String externalId) {
        cache.remove(externalId);
    }
}
