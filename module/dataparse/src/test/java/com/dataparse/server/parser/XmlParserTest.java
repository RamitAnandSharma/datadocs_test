package com.dataparse.server.parser;

import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.XmlParser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.upload.XmlFileDescriptor;
import com.dataparse.server.util.ResourceFileStorage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.dataparse.server.util.DeepEquals.deepEquals;

@Slf4j
public class XmlParserTest {

    private static final String XML_FILE = "test.xml";

    @Test
    public void countTest(){
        XmlFileDescriptor descriptor = new XmlFileDescriptor();
        descriptor.setPath(XML_FILE);

        descriptor.getSettings().setRowXPath("//person");
        Parser parser = new XmlParser(new ResourceFileStorage(), descriptor);
        Assert.assertEquals(Pair.of(1L, true), parser.getRowsEstimateCount(0));

        descriptor.getSettings().setRowXPath("//person/row");
        parser = new XmlParser(new ResourceFileStorage(), descriptor);
        Assert.assertEquals(Pair.of(2L, true), parser.getRowsEstimateCount(0));

        descriptor.getSettings().setRowXPath("//person/row/addresses/Address");
        parser = new XmlParser(new ResourceFileStorage(), descriptor);
        Assert.assertEquals(Pair.of(3L, true), parser.getRowsEstimateCount(0));
    }

    @Test
    public void parseTest() throws Exception {
        List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();

        List<Map<String, Object>> example = Arrays.asList(
                ImmutableMap.of(
                        "row.name", "Tom",
                        "row.addresses.Address[zip]", Arrays.asList("12345","45678"),
                        "row.addresses.Address.State", Arrays.asList("California","California"),
                        "row.addresses.Address.City", Arrays.asList("Los angeles","San Francisco")),
                ImmutableMap.of(
                        "row.name", "Jim",
                        "row.addresses.Address[zip]", "54321",
                        "row.addresses.Address.State", "NY",
                        "row.addresses.Address.City", "New York"));

        XmlFileDescriptor descriptor = new XmlFileDescriptor();
        descriptor.getSettings().setRowXPath("//person/row");
        descriptor.setPath(XML_FILE);
        Parser parser = new XmlParser(new ResourceFileStorage(), descriptor);
        parser.parse().forEachRemaining(x -> {
            try {
                log.info(new ObjectMapper().writeValueAsString(x));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            result.add(x);
        });
        Assert.assertTrue(deepEquals(example, result));
    }

}
