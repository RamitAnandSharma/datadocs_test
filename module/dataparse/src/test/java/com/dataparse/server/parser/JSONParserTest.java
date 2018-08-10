package com.dataparse.server.parser;

import com.dataparse.server.service.parser.JSONParser;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.DeepEquals;
import com.dataparse.server.util.ResourceFileStorage;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

@Slf4j
public class JSONParserTest {

    private static final String JSON_ARRAY_FILE = "test_array.json";
    private static final String JSON_OBJECT_FILE = "test_object.json";
    private static final String JSON_LARGE_FILE = "test_large.json";

    @Test
    public void countTest(){
        Parser parser = new JSONParser(new ResourceFileStorage(), new FileDescriptor(JSON_ARRAY_FILE));
        Assert.assertEquals(Pair.of(2L, true), parser.getRowsEstimateCount(0));
    }

    @Test
    public void countEstimateTest(){
        URL url = getClass().getClassLoader().getResource(JSON_LARGE_FILE);
        File file = new File(url.getFile());

        Parser parser = new JSONParser(new ResourceFileStorage(), new FileDescriptor(JSON_LARGE_FILE));
        log.info(String.valueOf(parser.getRowsEstimateCount(file.length())));
    }

    @Test
    public void jsonObjectParseTest() throws Exception{

        List<Map<String, Object>> objects = Arrays.asList(
                ImmutableMap.of(
                        "objectID", "T1",
                        "Language", Arrays.asList("English", "Portuguese", "Spanish"),
                        "Title.Name", "Divergent",
                        "DownloadURL.Name", Arrays.asList("EN Feature (component)", "EN Feature (for China)"),
                        "DownloadURL.Downloads.Name", Arrays.asList("Feature (EN)","Audio (ES)","Feature (EN)")));

        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        Parser parser = new JSONParser(new ResourceFileStorage(), new FileDescriptor(JSON_OBJECT_FILE));
        parser.parse().forEachRemaining(x -> {
            log.info(x.toString());
            result.add(x);
        });
        assertTrue(DeepEquals.deepEquals(result, objects));
    }

    @Test
    public void jsonArrayParseTest() throws Exception{
        List<Map<String, Object>> objects = Arrays.asList(
                ImmutableMap.of(
                        "objectID", "T1",
                        "Language", Arrays.asList("English", "Portuguese", "Spanish"),
                        "Title.Name", "Divergent",
                        "DownloadURL.Name", Arrays.asList("EN Feature (component)", "EN Feature (for China)"),
                        "DownloadURL.Downloads.Name", Arrays.asList("Feature (EN)", "Audio (ES)", "Feature (EN)")),
                ImmutableMap.of(
                        "objectID", "T2",
                        "Language", Arrays.asList("English", "Portuguese", "Spanish", "Russian"),
                        "Title.Name", "Insurgent",
                        "DownloadURL.Name", Arrays.asList("EN Feature (component)", "EN Feature (for China)"),
                        "DownloadURL.Downloads.Name", Arrays.asList("Feature (EN)", "Audio (ES)", "Feature (EN)")));

        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        Parser parser = new JSONParser(new ResourceFileStorage(), new FileDescriptor(JSON_ARRAY_FILE));
        parser.parse().forEachRemaining(x -> {
            log.info(x.toString());
            result.add(x);
        });
        assertTrue(DeepEquals.deepEquals(result, objects));
    }

}
