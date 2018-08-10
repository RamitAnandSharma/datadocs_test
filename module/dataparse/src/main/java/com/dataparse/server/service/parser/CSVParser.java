package com.dataparse.server.service.parser;

import com.dataparse.server.service.parser.assist_csv.CsvCharsetDetector;
import com.dataparse.server.service.parser.assist_csv.CsvDelimeterDetector;
import com.dataparse.server.service.parser.assist_csv.CsvHeadersDetector;
import com.dataparse.server.service.parser.assist_csv.CsvOffsetDetector;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.CsvFormatSettings;
import com.dataparse.server.util.InputStreamUtils;
import com.dataparse.server.util.ListUtils;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

import static com.dataparse.server.service.parser.RecordIteratorBuilder.with;


@Slf4j
public class CSVParser extends Parser {

    public static final String NULL_STRING_DEFAULT = "";
    public static final Character QUOTE_CHARACTER_DEFAULT = '"';
    public static final Character COMMENT_CHARACTER_DEFAULT = '#';
    public static final Character IGNORE_COMMENTS_CHAR = '\u0001';
    public static final List<Character> QUOTED_DELIMITERS = Arrays.asList(',', ';', '\t', '|');
    public static final List<Character> DELIMITERS = Arrays.asList(',', ';', '\t', '|', '\u0001');
    public static final Character ESCAPE_CHARACTER_DEFAULT = '\\';
    public static final String DEFAULT_ROW_DELIMITER = "\n";
    public static final List<String> ROW_DELIMITERS = Arrays.asList("\u0002\n", "\n", "\r\n", "\r");
    public static final String TOTAL_STR = "total";
    public static final String GRAND_TOTAL_STR = "grand total";

    private CsvOffsetDetector offsetDetector;
    private CsvHeadersDetector headersDetector;
    private CsvDelimeterDetector delimeterDetector;
    private CsvCharsetDetector charsetDetector;

    private FileStorage fileStorage;
    private String path;
    private CsvFileDescriptor descriptor;

    public CSVParser(FileStorage fileStorage, CsvFileDescriptor descriptor) {

        this.fileStorage = fileStorage;
        this.descriptor = descriptor;
        this.path = descriptor.getPath();

        offsetDetector = new CsvOffsetDetector(fileStorage, path);
        headersDetector = new CsvHeadersDetector(fileStorage, path);
        delimeterDetector = new CsvDelimeterDetector(fileStorage, path);
        charsetDetector = new CsvCharsetDetector(fileStorage, path);
    }

    public static boolean testCSV(FileStorage fs, String path) {
        String charset;

        try (InputStream is = fs.getFile(path)) {
            CsvCharsetDetector csvCharsetDetector = new CsvCharsetDetector(fs, path);
            charset = csvCharsetDetector.tryGetCharset(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (InputStream is = fs.getFile(path)) {
            CsvDelimeterDetector csvDelimeterDetector = new CsvDelimeterDetector(fs, path);
            return csvDelimeterDetector.tryGetDelimiter(is, charset) != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CsvFormatSettings tryGetCsvSettings() throws IOException {
        CsvFormatSettings settings = new CsvFormatSettings();

        settings.setNullString(NULL_STRING_DEFAULT);
        settings.setCommentCharacter(COMMENT_CHARACTER_DEFAULT);
        settings.setStartOnRow(1);
        settings.setSkipAfterHeader(0);

        long offset;
        settings.setCharset(charsetDetector.tryGetCharset());
        try(InputStream is = fileStorage.getFile(path)){
            offset = offsetDetector.getStartRowOffset(is, descriptor);
        }

        Pair<Character, String> delimiters = delimeterDetector.tryGetDelimiter(offset, settings.getCharset());
        settings.setDelimiter(delimiters.getLeft());
        settings.setRowDelimiter(delimiters.getRight());

        if(settings.getDelimiter() == null){
            settings.setSkipFromBottom(0);
            settings.setQuote(null);
        } else {
            Integer skipFromBottom = offsetDetector.tryGetSkipFromBottom(settings.getCharset(), settings.getDelimiter(), descriptor);
            settings.setSkipFromBottom(skipFromBottom);
            if (QUOTED_DELIMITERS.contains(settings.getDelimiter())) {
                settings.setQuote(QUOTE_CHARACTER_DEFAULT);
            } else {
                settings.setQuote(null);
            }
        }

        settings.setUseHeaders(headersDetector.tryGetHeadersUsage(offset, settings));
        settings.setEscape(charsetDetector.tryGetEscape(offset, settings.getCharset(), settings.getQuote()));
        settings.setSkipAfterHeader(offsetDetector.tryGetCommentsOffset(offset, settings));

        return settings;
    }

    public static CsvParserSettings buildFormat(CsvFormatSettings formatSettings){

        CsvParserSettings settings = new CsvParserSettings();

        settings.getFormat().setDelimiter(Optional.ofNullable(formatSettings.getDelimiter()).orElse(DELIMITERS.get(0)));
        settings.getFormat().setLineSeparator(Optional.ofNullable(formatSettings.getRowDelimiter()).orElse(DEFAULT_ROW_DELIMITER));
        settings.getFormat().setQuote(Optional.ofNullable(formatSettings.getQuote()).orElse(QUOTE_CHARACTER_DEFAULT));
        settings.getFormat().setComment(Optional.ofNullable(formatSettings.getCommentCharacter()).orElse(COMMENT_CHARACTER_DEFAULT));
        settings.getFormat().setQuoteEscape(Optional.ofNullable(formatSettings.getEscape()).orElse(ESCAPE_CHARACTER_DEFAULT));

        if(formatSettings.getRowDelimiter() != null && formatSettings.getRowDelimiter().equals("\u0002\n")){
            settings.getFormat().setNormalizedNewline('\u0002');
        }

        settings.setNullValue(null);
        settings.setEmptyValue(null);
        settings.setMaxCharsPerColumn(-1);

        return settings;
    }

    // ! impure, can force parser to skip first row

    @Override
    @SuppressWarnings("unchecked")
    public RecordIterator parse() throws IOException {

        long offset = offsetDetector.getStartRowOffset(descriptor);
        final CsvFormatSettings formatSettings = descriptor.getSettings();
        final CsvParserSettings settings = buildFormat(formatSettings);
        final List<IndexedParsedColumn> headersMap = headersDetector.tryGetHeaders(offset, formatSettings);
        final CsvParser parser = new CsvParser(settings);

        InputStream is = InputStreamUtils.wrapWithoutBOM(fileStorage.getFile(path));
        is.skip(offset);

        CountingInputStream cis = new CountingInputStream(is);
        Reader fileReader = new InputStreamReader(cis, Optional
                .ofNullable(formatSettings.getCharset())
                .orElse(Charsets.UTF_8.name()));
        parser.beginParsing(fileReader);

        // skip row from start and do not take it to data section
        if(headersMap.size() > 0) {
            parser.parseNext();
        }

        return with(new CsvRecordIterator(headersMap, parser, cis))
                .interruptible()
                .limited(descriptor.getLimit())
                .withSkippedRows(formatSettings.getSkipAfterHeader() + 1, formatSettings.getSkipFromBottom())
                .withTransforms(descriptor.getColumnTransforms())
                .withColumns(descriptor.getColumns())
                .build();
    }

}

class CsvRecordIterator implements RecordIterator {

    private Map<Integer, IndexedParsedColumn> headersMap;
    private CsvParser parser;
    private CountingInputStream cis;


    private String[] tmp;
    private Map<AbstractParsedColumn, Object> currentObj;

    CsvRecordIterator(List<IndexedParsedColumn> headersMap, CsvParser parser, CountingInputStream cis) {
        this.headersMap = ListUtils.groupByKey(headersMap, IndexedParsedColumn::getIndex);
        this.parser = parser;
        this.cis = cis;
    }

    @Override
    public void close() throws IOException {
        parser.stopParsing();
        cis.close();
    }

    @Override
    public boolean hasNext() {
        if(tmp == null) {
            tmp = parser.parseNext();
        }
        return tmp != null;
    }

    @Override
    public Map<AbstractParsedColumn, TypeDescriptor> getSchema() {
        return getSchema(null);
    }

    @Override
    public Map<AbstractParsedColumn, TypeDescriptor> getSchema(final Map<AbstractParsedColumn, TypeDescriptor> currentSchema) {
        Map<AbstractParsedColumn, Object> o = getRaw();
        if(o == null){
            return null;
        }
        Map<AbstractParsedColumn, TypeDescriptor> schema = new LinkedHashMap<>();
        for(Map.Entry<AbstractParsedColumn, Object> entry: o.entrySet()){
            TypeDescriptor type = DataType.tryParseType((String) entry.getValue(), currentSchema.get(entry.getKey()));
            schema.put(entry.getKey(), type);
        }
        return schema;
    }

    private Map<AbstractParsedColumn, Object> nextRaw() {
        String[] record;

        try {
            record = tmp == null ? parser.parseNext() : tmp;
            tmp = null;
        } catch (Exception e) {
            return null;
        }

        if(record == null) {
            return null;
        }

        Map<AbstractParsedColumn, Object> o = new LinkedHashMap<>(record.length);

        for(int i = 0; i < record.length; i++) {
            int currentColumn = i + 1;
            IndexedParsedColumn key = headersMap.getOrDefault(i, new IndexedParsedColumn(i, "Field " + currentColumn));
            String value = record[i];
            if("NULL".equals(value)) { // "NULL" strings should cast to null values as well
                value = null;
            }
            o.put(new IndexedParsedColumn(i, key.getColumnName()), value);
        }

        return o;
    }

    @Override
    public Map<AbstractParsedColumn, Object> getRaw() {
        return this.currentObj;
    }

    @Override
    public long getRowNumber() {
        return parser.getContext().currentLine();
    }

    @Override
    public Map<AbstractParsedColumn, Object> next() {
        this.currentObj = nextRaw();
        return this.currentObj;
    }

    @Override
    public long getBytesCount() {
        return cis.getByteCount();
    }

}
