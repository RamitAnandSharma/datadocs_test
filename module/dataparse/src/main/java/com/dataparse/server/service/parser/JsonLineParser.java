package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ColumnsUtils;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;

public class JsonLineParser extends Parser {

    private FileStorage fileStorage;
    private String path;
    private Descriptor descriptor;
    private TypeReference<Map<AbstractParsedColumn, Object>> TYPE_REF = new TypeReference<Map<AbstractParsedColumn, Object>>() {};

    public JsonLineParser(FileStorage fileStorage, FileDescriptor descriptor) {
        this.fileStorage = fileStorage;
        this.path = descriptor.getPath();
        this.descriptor = descriptor;
    }

    public String getPath() {
        return path;
    }

    public static String writeString(Map<AbstractParsedColumn, Object> o) {
        try {
            return JsonUtils.mapper.writeValueAsString(o) + "\n";
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordIterator parse() throws IOException {
        InputStream is = fileStorage.getFile(path);
        CountingInputStream cis = new CountingInputStream(is);
        BufferedReader br = new BufferedReader(new InputStreamReader(cis));
        return with(new RecordIterator() {

            String line;
            Map<AbstractParsedColumn, Object> currentObj;

            @Override
            public void close() throws IOException {
                br.close();
            }

            @Override
            public Map<AbstractParsedColumn, Object> getRaw() {
                return this.currentObj;
            }

            @Override
            public boolean hasNext() {
                if (StringUtils.isEmpty(line)) {
                    try {
                        line = br.readLine();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return line != null;
            }

            @Override
            public Map<AbstractParsedColumn, Object> next() {
                try {
                    if(StringUtils.isEmpty(line)){
                        line = br.readLine();
                    }
                    if(StringUtils.isEmpty(line)){
                        currentObj = null;
                        return null;
                    }
                    currentObj = JsonUtils.mapper.readValue(line, TYPE_REF);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                line = null;
                return currentObj;
            }

            @Override
            public long getBytesCount() {
                return cis.getByteCount();
            }
        })
                .limited(descriptor.getLimit())
                .withTransforms(descriptor.getColumnTransforms())
                .withColumns(descriptor.getColumns())
                .interruptible()
                .build();
    }
}
