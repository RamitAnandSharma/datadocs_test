package com.dataparse.server.parser;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.dataparse.server.service.parser.AvroParser;
import com.dataparse.server.service.parser.DataFormat;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.parser.type.DataType;
import com.dataparse.server.service.parser.type.TypeDescriptor;
import com.dataparse.server.service.parser.writer.AvroWriter;
import com.dataparse.server.service.upload.FileDescriptor;
import com.dataparse.server.util.DeepEquals;
import com.dataparse.server.util.LocalFileStorage;
import com.dataparse.server.util.ResourceFileStorage;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

@Slf4j
public class AvroParserTest {

  @Test
  public void test() throws Exception{
    List<Map<String, Object>> objects = Arrays.asList(
        ImmutableMap.of("username", "miguno", "tweet", "Rock: Nerf paper, scissors is fine.", "timestamp", 1366150681L),
        ImmutableMap.of("username", "BlizzardCS", "tweet", "Works as intended.  Terran is IMBA.", "timestamp", 1366154481L));

    ResourceFileStorage storage = new ResourceFileStorage();

    FileDescriptor avroDescriptor = new FileDescriptor();
    avroDescriptor.setPath("test.avro");
    avroDescriptor.setFormat(DataFormat.AVRO);

    AvroParser parser = new AvroParser(storage, avroDescriptor);
    RecordIterator it = parser.parse();
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    while(it.hasNext()){
      result.add(it.next());
    }

    assertTrue(DeepEquals.deepEquals(result, objects));
    log.info("Contents: {} ", result);
  }

  @Test
  public void writeData() throws Exception {
    String resultFile = "/tmp/writeDataTest.avro";

    ColumnInfo c0 = new ColumnInfo();
    c0.setAlias("c0");
    c0.setType(new TypeDescriptor(DataType.STRING));
    ColumnInfo c1 = new ColumnInfo();
    c1.setAlias("c1");
    c1.setType(new TypeDescriptor(DataType.STRING));
    FileOutputStream outputStream = new FileOutputStream(resultFile);

    AvroWriter writer = new AvroWriter(outputStream, Lists.newArrayList(c0, c1));
    writer.writeRecord(com.google.common.collect.ImmutableMap.of("c0", "1L", "c1", "somestring"));
    writer.writeRecord(com.google.common.collect.ImmutableMap.of("c0", "2L", "c1", "anotherstring"));

    writer.flush();
    writer.close();

    AvroParser avroParser = new AvroParser(new LocalFileStorage(), new FileDescriptor(resultFile));
    RecordIterator parse = avroParser.parse();
    List<Map<AbstractParsedColumn, Object>> parsedResults = new ArrayList<>();
    parse.forEachRemaining(parsedResults::add);
    System.out.println();

  }

}
