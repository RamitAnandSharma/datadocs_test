package com.dataparse.server.service.hazelcast;

import com.dataparse.server.service.bigquery.cache.serialization.*;
import com.dataparse.server.service.flow.cache.*;
import com.dataparse.server.service.flow.cache.serialization.*;
import com.dataparse.server.service.visualization.*;
import com.hazelcast.config.*;
import com.hazelcast.core.*;
import org.springframework.stereotype.*;

import javax.annotation.*;

@Service
public class HazelcastClient {

    private HazelcastInstance instance;

    @PostConstruct
    public void init(){
        Config config = new Config();

        config.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig()
                                             .setImplementation(new QueryResultSerializer())
                                             .setTypeClass(BigQueryResult.class))
                .addSerializerConfig(new SerializerConfig()
                                             .setImplementation(new TreeSerializer())
                                             .setTypeClass(Tree.class))
                .addSerializerConfig(new SerializerConfig()
                                             .setImplementation(new FlowPreviewResultSerializer())
                                             .setTypeClass(FlowPreviewResultCacheValue.class));

        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        NetworkConfig network = config.getNetworkConfig();
        network.getJoin().getTcpIpConfig().setEnabled(false);
        network.getJoin().getMulticastConfig().setEnabled(false);
        this.instance = Hazelcast.newHazelcastInstance(config);
    }

    @PreDestroy
    public void destroy(){
        this.instance.shutdown();
    }

    public HazelcastInstance getClient(){
        return instance;
    }
}
