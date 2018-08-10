package com.dataparse.server.service.es.index.id_provider;

import java.util.Map;
import java.util.UUID;

public class RandomIdProvider implements IdProvider {

    @Override
    public String getId(Map<String, Object> o) {
        return UUID.randomUUID().toString();
    }

}
