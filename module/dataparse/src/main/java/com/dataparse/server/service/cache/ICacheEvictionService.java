package com.dataparse.server.service.cache;

import java.util.List;

public interface ICacheEvictionService {
    /**
     * Adds the provided key to a global list of keys that we'll need later for eviction
     *
     * @param key the cached key for any entry
     */
    void addKeyToList(String key);

    /**
     * Find keys that contain the partial key
     *
     * @param partialKey the cached partial key for an entry
     * @return List of matching keys
     */
    List<String> findKeyByPartialKey(String partialKey);

    /**
     * Evicts the cache and key for an entry matching the provided key
     *
     * @param key the key of the entry you want to evict
     */
    void evict(String key);
}