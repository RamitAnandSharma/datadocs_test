package com.dataparse.server.service.es.index.id_provider;

import java.util.Map;

/**
 * Provides ID from data field.
 */
public class FieldIdProvider implements IdProvider {

    private String idField;

    public FieldIdProvider(String idField) {
        this.idField = idField;
    }

    @Override
    public String getId(Map<String, Object> o) {
        return o.get(idField).toString();
    }
}
