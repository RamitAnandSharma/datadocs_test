package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ColumnsUtils;
import com.dataparse.server.service.parser.exception.TooLargeFileException;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.Flatten;
import com.dataparse.server.util.JsonUtils;
import lombok.AllArgsConstructor;
import org.apache.commons.io.input.CountingInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

@AllArgsConstructor
public class JsonObjectParser extends Parser {
    private FileStorage fileStorage;
    private String path;
    private FileDescriptor descriptor;


    @Override
    public RecordIterator parse() throws IOException {
        if(!DataFormat.JSON_OBJECT.isFileSizeAcceptable(descriptor.getSize())) {
            throw new TooLargeFileException(descriptor.getOriginalFileName(), descriptor.getSize(),
                    DataFormat.JSON_OBJECT);
        }
        InputStream is = fileStorage.getFile(path);
        CountingInputStream cis = new CountingInputStream(is);
        BufferedReader br = new BufferedReader(new InputStreamReader(cis));
        return RecordIteratorBuilder.
                with(new JsonObjectIterator(br, descriptor.getSize())).
                withTransforms(descriptor.getColumnTransforms()).
                withColumns(descriptor.getColumns()).
                build();

    }

    private class JsonObjectIterator implements RecordIterator {

        private Map<AbstractParsedColumn, Object> obj;
        private BufferedReader reader;
        private Long fileSize;

        JsonObjectIterator(BufferedReader reader, Long fileSize) {
            this.reader = reader;
            this.fileSize = fileSize;
        }

        @SuppressWarnings("unchecked")
        private Map<AbstractParsedColumn, Object> readObject() {
            try {
                Map<String, Object> flatten = Flatten.flatten(JsonUtils.mapper.readValue(reader, Map.class));
                return ColumnsUtils.namedColumnsFromMap(flatten);
            } catch (IOException e) {
                throw new RuntimeException("Can not parse json object. ", e);
            }
        }

        @Override
        public Map<AbstractParsedColumn, Object> getRaw() {
            return obj;
        }

        @Override
        public long getBytesCount() {
            return fileSize;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public boolean hasNext() {
            return obj == null;
        }

        @Override
        public Map<AbstractParsedColumn, Object> next() {
            if(!hasNext()) {
                return null;
            }

            obj = readObject();
            return obj;
        }
    }
}
