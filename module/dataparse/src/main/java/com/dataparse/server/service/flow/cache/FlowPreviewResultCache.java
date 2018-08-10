package com.dataparse.server.service.flow.cache;

import com.dataparse.server.service.hazelcast.*;
import com.hazelcast.config.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;
import java.util.*;

@Service
public class FlowPreviewResultCache {

    private final static String FLOW_PREVIEW_CACHE = "FLOW_PREVIEW_CACHE";

    @Autowired
    private HazelcastClient hazelcast;

    @PostConstruct
    public void init(){
        MapConfig mapConfig = new MapConfig(FLOW_PREVIEW_CACHE);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setMaxSizeConfig(new MaxSizeConfig(10, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE));
        hazelcast.getClient().getConfig().addMapConfig(mapConfig);;
    }

    public void put(FlowPreviewResultCacheKey key, FlowPreviewResultCacheValue value) {
        this.hazelcast.getClient().getMap(FLOW_PREVIEW_CACHE).put(key.toString(), value);
    }

    public FlowPreviewResultCacheValue get(FlowPreviewResultCacheKey key) {
        return (FlowPreviewResultCacheValue) this.hazelcast.getClient().getMap(FLOW_PREVIEW_CACHE).get(key.toString());
    }

    public void evict(List<Long> bookmarkIds){
        this.hazelcast.getClient().getMap(FLOW_PREVIEW_CACHE).removeAll(
                entry -> {
                    String key = (String) entry.getKey();
                    Long bookmarkId = Long.parseLong(key.split(":")[0]);
                    return bookmarkIds.contains(bookmarkId);
                });
    }

}
