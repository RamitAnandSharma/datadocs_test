package com.dataparse.server.parser;

import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.parser.CompositeParser;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.upload.CompositeDescriptor;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.Descriptor;
import com.dataparse.server.service.upload.UploadService;
import com.dataparse.server.util.DeepEquals;
import com.dataparse.server.util.ResourceFileStorage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;

@Slf4j
public class CompositeParserTest {

    private static final String CSV_FILE1 = "union/1.csv";
    private static final String CSV_FILE2 = "union/2.csv";
    private static final String CSV_FILE3 = "union/3.csv";

    private static List<Map<String, Object>> objects = ImmutableList.of(
            ImmutableMap.of("id", 1.0, "name", "item", "param", "white"),
            ImmutableMap.of("id", 2.0, "name", "gizmo", "param", "blue"),
            ImmutableMap.of("id", 3.0, "name", "thing", "param", "yellow"),
            ImmutableMap.of("id", 4.0, "name", "object", "param", "green"),
            ImmutableMap.of("id", 5.0, "name", "entity", "param", "purple"),
            ImmutableMap.of("id", 6.0, "name", "article", "param", "pink")
    );

    @Test
    public void testCompositeParser() throws Exception {
        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
        List<Descriptor> descriptors = Arrays.asList(
                new CsvFileDescriptor(CSV_FILE1),
                new CsvFileDescriptor(CSV_FILE2),
                new CsvFileDescriptor(CSV_FILE3));
        for(Descriptor descriptor : descriptors){
            descriptor.setColumns(new UploadService().tryParseColumns(new CSVParser(new ResourceFileStorage(), (CsvFileDescriptor) descriptor)));
        }
        List<Parser> parsers = descriptors.stream().map(d -> new CSVParser(new ResourceFileStorage(), (CsvFileDescriptor) d)).collect(Collectors.toList());
        CompositeParser parser = new CompositeParser(new CompositeDescriptor(descriptors), parsers);
        parser.parse().forEachRemaining(x -> {
            log.info(x.toString());
            result.add(x);
        });
        assertTrue(DeepEquals.deepEquals(result, objects));
    }
}
