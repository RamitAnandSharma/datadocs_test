package com.dataparse.server.service.bigquery.cache;

import com.dataparse.server.service.bigquery.cache.serialization.*;
import com.dataparse.server.service.hazelcast.*;
import com.hazelcast.config.*;
import org.apache.commons.lang3.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;

import javax.annotation.*;

@Service
public class HazelcastQueryCache implements IQueryCache<String> {

    public final static String QUERY_CACHE = "QUERY_CACHE";

    @Autowired
    private HazelcastClient hazelcast;

    @PostConstruct
    public void init(){
        MapConfig mapConfig = new MapConfig(HazelcastQueryCache.QUERY_CACHE);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setMaxSizeConfig(new MaxSizeConfig(10, MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE));
        hazelcast.getClient().getConfig().addMapConfig(mapConfig);
    }

    @Override
    public void put(final String key, final BigQueryResult obj) {
        this.hazelcast.getClient().getMap(QUERY_CACHE).put(key, obj);
    }

    @Override
    public BigQueryResult get(final String key) {
        return (BigQueryResult) this.hazelcast.getClient().getMap(QUERY_CACHE).get(key);
    }

    @Override
    public void evict(final String externalId) {
        throw new NotImplementedException("Cache entries are invalidated automatically");
    }
}
