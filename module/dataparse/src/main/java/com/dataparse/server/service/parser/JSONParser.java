package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ColumnsUtils;
import com.dataparse.server.service.parser.iterator.*;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.Flatten;
import com.dataparse.server.util.ListUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.*;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;

@Slf4j
public class JSONParser extends Parser {

    private static final Set<Character> QUOTE_OR_CLOSING_BRACKET = Sets.newHashSet('"', '}');
    private static final Set<Character> WHITESPACE_CHARS = Sets.newHashSet(' ', '\t', '\r', '\n');

    private FileStorage fileStorage;
    private FileDescriptor descriptor;
    private Map<String, ColumnInfo> columnsByNames;

    public JSONParser(FileStorage fileStorage, FileDescriptor descriptor) {
        this.fileStorage = fileStorage;
        this.descriptor = descriptor;
        this.columnsByNames = ListUtils.groupByKey(this.descriptor.getColumns(), ColumnInfo::getName);
    }

    public static boolean testJSONObject(FileStorage fs, String path) {
        try (InputStream is = fs.getFile(path)) {
            return DataFormat.JSON_OBJECT.equals(testJSONFormat(is));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static boolean testJSONArray(FileStorage fs, String path) {
        try (InputStream is = fs.getFile(path)) {
            return DataFormat.JSON_ARRAY.equals(testJSONFormat(is));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static DataFormat testJSONFormat(InputStream is){
        Reader reader = new InputStreamReader(is);

        char c = getNextNonWhitespaceChar(reader);
        switch (c){
            case '{': {
                char secondCharacter = getNextNonWhitespaceChar(reader);
                if(QUOTE_OR_CLOSING_BRACKET.contains(secondCharacter)){
                    return DataFormat.JSON_OBJECT;
                }
                break;
            }
            case '[': return DataFormat.JSON_ARRAY;
        }
        return null;
    }

    public static char getNextNonWhitespaceChar(Reader reader) {
        try {
            int i = 0, c = reader.read();
            while (c != -1 && i++ < 100 && WHITESPACE_CHARS.contains((char) c)) {
                c = reader.read();
            }
            return (char) c;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static Object parseValueFromString(JsonParser jsonParser) throws IOException {
        switch (jsonParser.getCurrentToken()){
            case VALUE_NULL:
                return null;
            case VALUE_FALSE:
                return false;
            case VALUE_TRUE:
                return true;
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
                return jsonParser.getDoubleValue();
            case VALUE_STRING:
            default:
                // todo parse date?
                return jsonParser.getValueAsString();
        }
    }

    public String parseArray(JsonParser jsonParser, String p) throws IOException {
        List<Object> array = new ArrayList<>();
        while(jsonParser.nextToken() != JsonToken.END_ARRAY){
            if(jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                array.add(parseObject(jsonParser, p + "."));
            } else if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                array.add(parseArray(jsonParser, p));
            } else {
                array.add(parseValueFromString(jsonParser));
            }
        }
        return StringUtils.join(array, ",");
    }

    public Map<String, Object> parseObject(JsonParser jsonParser, String p) throws IOException {
        Map<String, Object> o = new LinkedHashMap<>();
        while(jsonParser.nextToken() != JsonToken.END_OBJECT){
            if(jsonParser.getCurrentToken() != JsonToken.FIELD_NAME){
                String f = jsonParser.getCurrentName();
                if(jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                    o.put(f, parseObject(jsonParser, p + f + "."));
                } else if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                    ColumnInfo columnInfo = columnsByNames.get(f);

                    if(columnInfo != null) {
                        columnInfo.setRepeated(true);
                        columnInfo.getSettings().setSplitOn("\u2404");
                    }

                    o.put(f, parseArray(jsonParser, p + f));
                } else {
                    o.put(f, parseValueFromString(jsonParser));
                }
            }
        }
        return o;
    }

    public RecordIterator parseJsonArray(CountingInputStream cis) throws IOException {
        JsonParser jsonParser = new JsonFactory().createParser(cis);
        jsonParser.nextToken();

        return with(new RecordIterator() {

            JsonToken nextToken;
            Map<AbstractParsedColumn, Object> currentObj;

            @Override
            public void close() throws IOException {
                jsonParser.close();
            }

            @Override
            public Map<AbstractParsedColumn, Object> getRaw() {
                return this.currentObj;
            }

            private void tryGoToNextObject(){
                if(nextToken == null){
                    try {
                        nextToken = jsonParser.nextToken();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                tryGoToNextObject();
                return nextToken != null && nextToken != JsonToken.END_ARRAY;
            }

            @Override
            public Map<AbstractParsedColumn, Object> next() {
                if(!hasNext()){
                    return null;
                }
                try {
                    this.nextToken = null;
                    this.currentObj = ColumnsUtils.namedColumnsFromMap(Flatten.flatten(parseObject(jsonParser, "")));
                    return this.currentObj;
                }
                catch (IOException e){
                    throw new RuntimeException(e);
                }
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

    @Override
    public RecordIterator parse() throws IOException {
        DataFormat format = testJSONFormat(fileStorage.getFile(descriptor.getPath()));
        InputStream is = fileStorage.getFile(descriptor.getPath());
        CountingInputStream cis = new CountingInputStream(is);

        if(DataFormat.JSON_OBJECT.equals(format)){
            return new JsonObjectParser(fileStorage, descriptor.getPath(), descriptor).parse();
        } else {
            return parseJsonArray(cis);
        }
    }
}
