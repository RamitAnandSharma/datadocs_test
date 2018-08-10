package com.dataparse.server.service.parser.assist_csv;

import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.parser.column.IndexedParsedColumn;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.CsvFormatSettings;
import com.dataparse.server.util.ArrayUtils;
import com.dataparse.server.util.InputStreamUtils;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class CsvHeadersDetector extends AbstractCsvDetector {

    private static final String STRIP_CHARACTERS = "#/\\";

    public CsvHeadersDetector(FileStorage fs, String path) {
        super(fs, path);
    }

    public boolean tryGetHeadersUsage(long offset, CsvFormatSettings settings) throws IOException {
        return tryGetHeaders(offset, settings) != null;
    }

    public List<IndexedParsedColumn> tryGetHeaders(long offset, CsvFormatSettings settings) throws IOException {
        if(settings.getUseHeaders() == null || settings.getUseHeaders()) {

            List<IndexedParsedColumn> headers = tryGetHeadersFromFirstRow(offset, settings);

            // give a try for comments section (only if we start from initial FIRST row)
            if (headers == null && offset == 0) {

                Character previousCommentSeparator = settings.getCommentCharacter();
                settings.setCommentCharacter(CSVParser.IGNORE_COMMENTS_CHAR);
                headers = tryGetHeadersFromFirstRow(offset, settings);

                if (headers == null) {
                    settings.setCommentCharacter(previousCommentSeparator);
                }
            }

            return stripHeaders(headers);
        } else {
            return new ArrayList<>();
        }
    }

    private List<IndexedParsedColumn> tryGetHeadersFromFirstRow(long offset, CsvFormatSettings settings)  throws IOException {
        try (InputStream is = fileStorage.getFile(path)) {
            is.skip(offset);
            return detectHeadersInFirstRow(is, settings);
        }
    }

    private List<IndexedParsedColumn> detectHeadersInFirstRow(InputStream is, CsvFormatSettings settings) throws IOException {
        try(InputStream noBomIs = InputStreamUtils.wrapWithoutBOM(is);
            BufferedReader reader = new BufferedReader(new InputStreamReader(noBomIs, settings.getCharset()))) {
            CsvParserSettings format = CSVParser.buildFormat(settings);
            CsvParser parser = new CsvParser(format);
            parser.beginParsing(reader);
            List<IndexedParsedColumn> headers = ArrayUtils.arrayToList(parser.parseNext(), IndexedParsedColumn::new);

            for(IndexedParsedColumn header : headers) {
                TypeDescriptor type = DataType.tryParseType(header.getColumnName());
                if(StringUtils.isBlank(header.getColumnName()) || type == null || !type.getDataType().equals(DataType.STRING)){
                    return null;
                }
            }

            return headers;
        }
    }

    private List<IndexedParsedColumn> stripHeaders(List<IndexedParsedColumn> headers) {
        if(headers == null) {
            return null;
        } else if (headers.size() == 0) {
            return headers;
        } else {
            for(IndexedParsedColumn header : headers) {
                header.setColumnName(StringUtils.strip(header.getColumnName(), STRIP_CHARACTERS));
            }
            return headers;
        }
    }


}
