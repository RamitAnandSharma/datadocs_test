package com.dataparse.server.parser;


import com.dataparse.server.service.parser.JsonLineParser;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.NamedParsedColumn;
import com.dataparse.server.service.upload.FileDescriptor;
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
import java.util.Date;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;


@Slf4j
public class JsonLineParserTest {

  private static final String JOIN_FILE = "test_lines.json";

  @Test
  public void countTest() {
    Parser parser = new JsonLineParser(new ResourceFileStorage(), new FileDescriptor(JOIN_FILE));
    Assert.assertEquals(Pair.of(4L, true), parser.getRowsEstimateCount(0));
  }

  @Test
  public void parse() throws IOException {

    List<Map<String, Object>> objects = ImmutableList.of(
        ImmutableMap.of("id", 2, "left_name", "bar", "right_name", "foo"),
        ImmutableMap.of("id", 4, "left_name", "bar", "right_name", "foo"),
        ImmutableMap.of("id", 1, "left_name", "bar", "right_name", "foo"),
        ImmutableMap.of("id", 3, "left_name", "bar", "right_name", "foo")
        );

    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    Parser parser = new JsonLineParser(new ResourceFileStorage(), new FileDescriptor(JOIN_FILE));
    parser.parse().forEachRemaining(x -> {
      log.info(x.toString());
      result.add(x);
    });
    assertTrue(DeepEquals.deepEquals(result, objects));

  }

  @Test
  public void write() throws IOException {
    Map<AbstractParsedColumn, Object> objects = ImmutableMap.of(
        new NamedParsedColumn("id"), 2,
        new NamedParsedColumn("left_name"), "bar",
        new NamedParsedColumn("right_name"), "foo");
    long begin = new Date().getTime();
    for (int i = 0; i < 100_000; i++) {
      JsonLineParser.writeString(objects);
    }
    log.info("100_000 modification map to json takes:" + (new Date().getTime() - begin)+" ms");

  }


}