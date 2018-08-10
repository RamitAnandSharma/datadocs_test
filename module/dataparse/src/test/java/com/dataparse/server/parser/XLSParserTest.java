package com.dataparse.server.parser;

import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.XLSParser;
import com.dataparse.server.service.parser.XLSXParser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.upload.XlsFileDescriptor;
import com.dataparse.server.service.upload.XlsFormatSettings;
import com.dataparse.server.util.DeepEquals;
import com.dataparse.server.util.ResourceFileStorage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

@Slf4j
public class XLSParserTest {

    private static final String XLS_FILE = "test.xls";
    private static final String XLSX_FILE = "test.xlsx";
    private static final String HUNDRED_COLUMNS_FILE = "hundred_columns_with_formulas.xlsx";

//    private static final String XLS_FILE_HARD = "hard.xls";
//    private static final String XLSX_FILE_HARD = "hard.xlsx";

    private static List<Map<String, Object>> objects = ImmutableList.of(
            ImmutableMap.of("A1", 1.0, "A2", 2.0),
            ImmutableMap.of("A1", 3.0, "A2", 4.0),
            ImmutableMap.of("A1", 5.0, "A2", 3.0),
            ImmutableMap.of("A1", 6.0, "A2", 12.0)
    );

    @Test
    public void xlsNoHeadersTest(){
        XlsFileDescriptor fileDescriptor = new XlsFileDescriptor(XLS_FILE);
        fileDescriptor.setSettings(new XlsFormatSettings());
        fileDescriptor.getSettings().setUseHeaders(false);
        Parser parser = new XLSParser(new ResourceFileStorage(), fileDescriptor);
        Assert.assertEquals(Pair.of(4L, true), parser.getRowsEstimateCount(0));
    }

    @Test
    public void parseWithFormulasTest() throws IOException {
        XlsFileDescriptor fileDescriptor = new XlsFileDescriptor(HUNDRED_COLUMNS_FILE);
        fileDescriptor.setSettings(new XlsFormatSettings());
        fileDescriptor.getSettings().setUseHeaders(true);
        Parser parser = new XLSXParser(new ResourceFileStorage(), fileDescriptor);
        RecordIterator parse = parser.parse();
        int i = 0;
        while (i++ < 1000 && parse.hasNext()) {
            parse.next();
        }
    }

    @Test
    public void xlsxNoHeadersTest(){
        XlsFileDescriptor fileDescriptor = new XlsFileDescriptor(XLSX_FILE);
        fileDescriptor.setSettings(new XlsFormatSettings());
        fileDescriptor.getSettings().setUseHeaders(false);
        Parser parser = new XLSXParser(new ResourceFileStorage(), fileDescriptor);
        Assert.assertEquals(Pair.of(4L, true), parser.getRowsEstimateCount(0));
    }

    @Test
    public void readXLS() throws IOException {
        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        Parser parser = new XLSParser(new ResourceFileStorage(), new XlsFileDescriptor(XLS_FILE));
        parser.parse().forEachRemaining(x -> {
            log.info(x.toString());
            result.add(x);
        });
        assertTrue(DeepEquals.deepEquals(result, objects));
    }

    @Test
    public void readXLSX() throws IOException {
        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        Parser parser = new XLSXParser(new ResourceFileStorage(), new XlsFileDescriptor(XLSX_FILE));
        parser.parse().forEachRemaining(x -> {
            log.info(x.toString());
            result.add(x);
        });
        assertTrue(result.size() == 4);
        for(int i=0;i<4;i++){
            assertTrue(result.get(i).equals(objects.get(i)));
        }
    }
}
