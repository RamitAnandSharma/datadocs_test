package com.dataparse.server.service.parser.assist_csv;

import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.storage.FileStorage;
import com.dataparse.server.util.InputStreamUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.mozilla.universalchardet.UniversalDetector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class CsvCharsetDetector extends AbstractCsvDetector {

    public CsvCharsetDetector(FileStorage fs, String path) {
        super(fs, path);
    }

    public String tryGetCharset(InputStream is) throws IOException {
        UniversalDetector detector = new UniversalDetector();

        byte[] buf = new byte[4096];
        int nread;
        while ((nread = is.read(buf)) > 0 && nread < 8192 && !detector.isDone()) {
            detector.handleData(buf, 0, nread);
        }
        detector.dataEnd();
        String encoding = detector.getDetectedCharset();
        if (encoding != null) {
            return encoding;
        }
        return Charsets.UTF_8.name();
    }

    public Character tryGetEscape(long offset, String charsetName, Character quote) throws IOException {
        try (InputStream is = fileStorage.getFile(path)) {
            is.skip(offset);
            return tryGetEscape(is, charsetName, quote);
        }
    }

    public String tryGetCharset() throws IOException {
        try (InputStream is = fileStorage.getFile(path)) {
            return tryGetCharset(is);
        }
    }

    // ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###

    private Character tryGetEscape(InputStream is, String charsetName, Character quote) throws IOException {
        if(quote == CSVParser.QUOTE_CHARACTER_DEFAULT) {
            try(InputStream noBomIs = InputStreamUtils.wrapWithoutBOM(is);
                BufferedReader reader = new BufferedReader(new InputStreamReader(noBomIs, charsetName))) {
                int rowNumber = 0;
                String line;
                while ((line = reader.readLine()) != null && rowNumber++ < 1000) {
                    int quoteMatchesCount = StringUtils.countMatches(line, CSVParser.QUOTE_CHARACTER_DEFAULT);
                    if (line.contains("&quot;") && quoteMatchesCount > 0){
                        return null;
                    }
                }
                return CSVParser.ESCAPE_CHARACTER_DEFAULT;
            }
        }
        return null;
    }

}

