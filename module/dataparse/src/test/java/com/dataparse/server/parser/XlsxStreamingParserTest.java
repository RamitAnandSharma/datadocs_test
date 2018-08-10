package com.dataparse.server.parser;

import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.XlsxStreamingParser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import com.dataparse.server.util.ResourceFileStorage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

@Slf4j
public class XlsxStreamingParserTest {

    private static final String XLSX_FILE = "test.xlsx";

    private static List<Map<String, Object>> objects;
    static {
        FastDateFormat f = FastDateFormat.getInstance("yyyy-dd-MM", TimeZone.getTimeZone("UTC"));
        try {
            objects = ImmutableList.of(
                    ImmutableMap.of("D1", 2L, "D2", "A2", "D3", f.parse("2017-02-01"), "D4", 1.3, "D5", 3L));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void readXLSX() throws IOException {
        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        ResourceFileStorage storage = new ResourceFileStorage();
        Map<String, String> sheets;
        try (XlsxStreamingParser parser = new XlsxStreamingParser(storage, new XlsFileDescriptor(XLSX_FILE))){
            sheets = parser.getSheets();
            log.info("Sheets: {}", sheets);
        }
        XlsFileDescriptor sheetDescriptor = new XlsFileDescriptor(XLSX_FILE);
        sheetDescriptor.setSheetName("rId5");
        sheetDescriptor.getSettings().setStartOnRow(2);
        sheetDescriptor.getSettings().setSkipAfterHeader(1);
        sheetDescriptor.getSettings().setSkipFromBottom(2);
        sheetDescriptor.getSettings().setUseHeaders(true);
        try(Parser parser = new XlsxStreamingParser(storage, sheetDescriptor)){
            Pair<Long, Boolean> rowCount = parser.getRowsEstimateCount(-1);
            assertNotNull(rowCount);
            log.info("Row count: {}", rowCount.getLeft());
            assertTrue(rowCount.getLeft().equals(1L));

            parser.parse().forEachRemaining(x -> {
                log.info(x.toString());
                result.add(x);
            });
        }

        assertTrue(result.size() == 1);
        assertTrue(result.get(0).equals(objects.get(0)));
    }

}
