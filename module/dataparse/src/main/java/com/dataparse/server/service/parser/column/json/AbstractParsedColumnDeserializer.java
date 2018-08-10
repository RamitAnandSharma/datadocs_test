package com.dataparse.server.service.parser.column.json;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.util.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public class AbstractParsedColumnDeserializer extends KeyDeserializer {

    public static SimpleModule buildModule() {
        SimpleModule module = new SimpleModule();
        module.addKeyDeserializer(AbstractParsedColumn.class, new AbstractParsedColumnDeserializer());
        return module;
    }

    @Override
    public AbstractParsedColumn deserializeKey(String s, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        return JsonUtils.readValue(s, AbstractParsedColumn.class);
    }
}
