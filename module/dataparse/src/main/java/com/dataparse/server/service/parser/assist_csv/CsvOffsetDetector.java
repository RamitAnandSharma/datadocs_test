package com.dataparse.server.service.parser.assist_csv;

import avro.shaded.com.google.common.collect.Lists;
import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.CsvFormatSettings;
import com.dataparse.server.util.InputStreamUtils;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CsvOffsetDetector extends AbstractCsvDetector {

    private static final Long AFTER_HEADERS_COMMENTS_SKIP_MAX = 100L;

    public CsvOffsetDetector(FileStorage fs, String path) {
        super(fs, path);
    }

    public int tryGetSkipFromBottom(String charset, Character delimiter, CsvFileDescriptor descriptor) throws IOException {
        byte[] lastBufferedContents = (byte[]) descriptor.getOptions().get("LAST_BUFFERED_CONTENTS");
        return tryGetSkipFromBottom(lastBufferedContents, charset, delimiter);
    }

    public Long getStartRowOffset(CsvFileDescriptor descriptor) throws IOException {
        Long offset;

        try(InputStream is = fileStorage.getFile(path)){
            offset = getStartRowOffset(is, descriptor);
        }

        return offset;
    }

    public Long getStartRowOffset(InputStream is, CsvFileDescriptor descriptor) throws IOException {
        try(InputStream noBomIs = InputStreamUtils.wrapWithoutBOM(is)) {
            CsvFormatSettings formatSettings = descriptor.getSettings();
            if (formatSettings != null && formatSettings.getStartOnRow() != null && formatSettings.getStartOnRow() > 1) {
                int counter = formatSettings.getStartOnRow();
                int c;
                long offset = 0;
                while (counter > 1) {
                    if ((c = noBomIs.read()) != -1) {
                        offset++;
                        if (c == '\n') {
                            counter--;
                        } else if (c == '\r') {
                            counter--;
                            if (noBomIs.read() != '\n') {
                                if (counter > 1) {
                                    offset++;
                                }
                            }
                        }
                    }
                }
                return offset;
            } else {
                return 0L;
            }
        }
    }

    // comments count between headers and data segment
    public Integer tryGetCommentsOffset(long offset, CsvFormatSettings settings) throws IOException {
        try (InputStream is = fileStorage.getFile(path)) {

            Integer skip = settings.getSkipAfterHeader();
            is.skip(offset + skip);

            try (InputStream noBomIs = InputStreamUtils.wrapWithoutBOM(is);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(noBomIs, settings.getCharset()))) {

                CsvParserSettings format = CSVParser.buildFormat(settings);
                CsvParser parser = new CsvParser(format);
                parser.beginParsing(reader);

                String[] row = parser.parseNext();
                Integer checkedRows = 0;

                while(checkedRows < AFTER_HEADERS_COMMENTS_SKIP_MAX) {
                    if(!isRowCommented(row)) {
                        return skip;
                    }

                    row = parser.parseNext();
                    checkedRows ++;
                    skip ++;
                }
            }

            return skip;
        }
    }

    private boolean isRowCommented(String[] rowData) {
        if(rowData != null && rowData.length > 0) {
            return rowData[0].startsWith(CSVParser.COMMENT_CHARACTER_DEFAULT.toString());
        } else {
            return false;
        }
    }

    private int tryGetSkipFromBottom(byte[] contents, String charset, Character delimiter) throws IOException {
        if(contents == null){
            return 0;
        }
        List<String> lines = IOUtils.readLines(new ByteArrayInputStream(contents), charset);
        List<String> lastLines = Lists.reverse(lines.stream().skip(1).collect(Collectors.toList())) // always skip first row because it might be cut
                .stream()
                .limit(10)
                .collect(Collectors.toList());
        boolean foundTotalRow = false;
        for(int i = 0; i < lastLines.size(); i++) {
            String delimiterRegex = Pattern.quote(delimiter + "");
            String line = lastLines.get(i);
            String[] lineValues = line.split(delimiterRegex);
            if(lineValues.length == 0){
                continue;
            }
            String firstValue = lineValues[0];
            if(firstValue.toLowerCase().contains(CSVParser.TOTAL_STR) || firstValue.toLowerCase().contains(CSVParser.GRAND_TOTAL_STR)){
                foundTotalRow = true;
                continue;
            }
            if(foundTotalRow){
                if(!line.replaceAll(delimiterRegex, "").trim().isEmpty()){
                    return i;
                }
            }
        }
        return foundTotalRow ? lastLines.size() : 0;
    }

}
