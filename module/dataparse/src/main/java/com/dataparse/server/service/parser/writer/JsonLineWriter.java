package com.dataparse.server.service.parser.writer;

import com.dataparse.server.util.JsonUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class JsonLineWriter implements RecordWriter {

    private OutputStream writer;

    public JsonLineWriter(OutputStream writer) {
        this.writer = writer;
    }

    @Override
    public void writeRecord(final Map<String, Object> o) throws IOException {
        writer.write(JsonUtils.mapper.writeValueAsString(o).getBytes("UTF-8"));
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws Exception {
        writer.close();
    }
}
