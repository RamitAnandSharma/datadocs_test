package com.dataparse.server.service.parser.column.json;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.util.JsonUtils;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import java.io.IOException;
import java.io.StringWriter;

public class AbstractParsedColumnSerializer extends JsonSerializer<AbstractParsedColumn> {
    @Override
    public void serialize(AbstractParsedColumn value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
        StringWriter writer = new StringWriter();
        JsonUtils.mapper.writeValue(writer, value);
        jgen.writeFieldName(writer.toString());
    }
}
