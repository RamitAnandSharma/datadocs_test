package com.dataparse.server.service.es.index.id_provider;

import java.util.Map;

public interface IdProvider {

    String getId(Map<String, Object> o);
}
