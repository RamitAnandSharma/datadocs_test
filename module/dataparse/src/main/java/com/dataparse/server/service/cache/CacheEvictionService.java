package com.dataparse.server.service.cache;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

@Service
public class CacheEvictionService implements ICacheEvictionService {
    private LinkedHashSet<String> cachedKeys = new LinkedHashSet<>();

    @Override
    public void addKeyToList(String key) {
        this.cachedKeys.add(key);
    }

    @Override
    public List<String> findKeyByPartialKey(String partialKey) {
        List<String> foundKeys = new ArrayList<>();
        for (String cachedKey : this.cachedKeys) {
            if (cachedKey.contains(partialKey)) {
                foundKeys.add(cachedKey);
            }
        }
        return foundKeys;
    }

    @Override
    @CacheEvict(value = "shareWithUsers", key = "#key")
    public void evict(String key) {
        this.cachedKeys.remove(key);
    }
}