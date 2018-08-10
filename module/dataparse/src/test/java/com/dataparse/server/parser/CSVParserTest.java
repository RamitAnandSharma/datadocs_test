package com.dataparse.server.parser;

import avro.shaded.com.google.common.collect.Lists;
import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.writer.CSVWriter;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.CsvFormatSettings;
import com.dataparse.server.service.upload.UploadService;
import com.dataparse.server.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

@Slf4j
public class CSVParserTest {

  private static final String CSV_FILE = "src/test/resources/test.csv";
  private static final String CUSTOM_NEWLINE_CSV_FILE = "test_custom_newline.csv";
  private static final String UTF_16_CSV_FILE = "test-utf16.csv";
  private static final String CSV_FILE_NO_HEADER = "test.txt";
  private static final String SKIP_ROWS_CSV_FILE = "test_skip_rows.csv";
  private static final String LARGE_CSV_FILE = "test_large.csv";
  private static final String BOOL_CSV_FILE = "test_bool.csv";
  private static final String CSV_FILE_WITH_TOTALS = "test_with_totals.csv";
  private static final String CSV_FILE_WITH_NULLS = "test_with_nulls.csv";

  private static List<Map<String, Object>> objects = ImmutableList.of(
      ImmutableMap.of("id", 1.0, "name", "item", "param", "white"),
      ImmutableMap.of("id", 2.0, "name", "gizmo", "param", "blue"),
      ImmutableMap.of("id", 3.0, "name", "thing", "param", "yellow"),
      ImmutableMap.of("id", 4.0, "name", "object", "param", "green"),
      ImmutableMap.of("id", 5.0, "name", "entity", "param", "purple"),
      ImmutableMap.of("id", 6.0, "name", "article", "param", "pink")
      );

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> csvWithNulls = ImmutableList.of(
      new HashMap(){{put("id", 1.0); put("name", "item"); put("param", null);}},
      ImmutableMap.of("id", 2.0, "name", "gizmo", "param", "blue"),
      new HashMap(){{put("id", null); put("name", "thing"); put("param", null);}},
      ImmutableMap.of("id", 4.0, "name", "object", "param", "green"),
      new HashMap(){{put("id", null); put("name", "entity"); put("param", "purple");}},
      ImmutableMap.of("id", 6.0, "name", "article", "param", "pink"));

  @Test
  public void countTest(){
    CsvFileDescriptor descriptor = new CsvFileDescriptor(CSV_FILE);
    Parser parser = new CSVParser(new ResourceFileStorage(), descriptor);
    Assert.assertEquals(Pair.of(6L, true), parser.getRowsEstimateCount(0));
  }

  @Test
  public void noHeadersTest(){
    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor(CSV_FILE);
    fileDescriptor.getSettings().setUseHeaders(false);
    Parser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    Assert.assertEquals(Pair.of(7L, true), parser.getRowsEstimateCount(0));
  }

  @Test
  public void combinedCsvTest() throws IOException {
    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor("combined-100-lines-copy.csv");
    fileDescriptor.getSettings().setUseHeaders(true);
    Parser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    List<Map> result = new ArrayList<>();
    parser.parse().forEachRemaining(result::add);
    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void writeCsvWithDateTest() throws IOException {
    List<String> header = Arrays.asList("id", "date");
    Map<String, Object> dataForInsert = ImmutableMap.of("id", "1", "date", new DateTime(2017, 11, 11, 0, 0 ,0, 0).toDate());
    File file = File.createTempFile(UUID.randomUUID().toString(), ".csv");
    OutputStream writer = new FileOutputStream(file);
    CSVWriter csvWriter = new CSVWriter(writer, header);
    csvWriter.writeRecord(dataForInsert);

    CsvFileDescriptor descriptor = new CsvFileDescriptor(file.getAbsolutePath());
    Parser parser = new CSVParser(new LocalFileStorage(), descriptor);
    writer.flush();
    writer.close();
    RecordIterator parse = parser.parse();
    Map<AbstractParsedColumn, Object> row = parse.next();
    Assert.assertEquals(row.get("date"), "2017-11-11 00:00:00.000");
  }

  @Test
  public void autoDetectNoHeaderTest() throws Exception {
    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor(CSV_FILE_NO_HEADER);
    CSVParser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    fileDescriptor.setSettings(parser.tryGetCsvSettings());
    RecordIterator it = parser.parse();
    List<Map<AbstractParsedColumn, Object>> result = Lists.newArrayList(it);
    Assert.assertEquals(682, result.size());
  }

  @Test
  public void skipRowsTest() throws IOException {
    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor(SKIP_ROWS_CSV_FILE);
    CSVParser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    fileDescriptor.setSettings(parser.tryGetCsvSettings());
    fileDescriptor.getSettings().setStartOnRow(4);
    fileDescriptor.getSettings().setUseHeaders(true);
    Assert.assertEquals(Pair.of(998L, true), parser.getRowsEstimateCount(0));
  }

  @Test
  public void estimatedCountTest() throws IOException {
    URL url = getClass().getClassLoader().getResource(LARGE_CSV_FILE);
    File file = new File(url.getFile());
    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor(LARGE_CSV_FILE);
    CSVParser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    fileDescriptor.setSettings(parser.tryGetCsvSettings());
    log.info(String.valueOf(parser.getRowsEstimateCount(file.length())));
  }

  @Test
  public void utf16EncodingTest() throws Exception {
    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor(UTF_16_CSV_FILE);
    fileDescriptor.getSettings().setCharset("UTF-16LE");
    fileDescriptor.getSettings().setDelimiter('\t');
    Parser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    parser.parse().forEachRemaining(x -> {
      log.info(x.toString());
      result.add(x);
    });
    Map<String, Object> line = new LinkedHashMap<>();
    line.put("ComponentId", "153340277");
    line.put("LicensorId", "1497934271");
    line.put("Title", "Battlefield Friends S05");
    line.put("Eidr", null);
    line.put("Upc", null);
    line.put("Isan", null);
    line.put("Pid", "MAC_BFF_S5");
    line.put("MediaId", "75c92309-0100-11db-89ca-0019b92a3933");
    line.put("Duration", "00:00:00");
    line.put("PositionInParent", "5");
    line.put("ContentOwner", null);
    line.put("ReleaseDate", null);
    line.put("AvailableInTerritories", "AU, CA, GB, US");
    assertEquals(line, result.get(0));
    log.info("{}", result);
  }

  @Test
  public void autodetectEncodingTest() throws Exception {
    CSVParser parser = new CSVParser(new ResourceFileStorage(), new CsvFileDescriptor(UTF_16_CSV_FILE));
    CsvFormatSettings settings = parser.tryGetCsvSettings();
    assertEquals(Charsets.UTF_16LE.name(), settings.getCharset());

    parser = new CSVParser(new ResourceFileStorage(), new CsvFileDescriptor(CSV_FILE));
    settings = parser.tryGetCsvSettings();
    assertEquals(Charsets.UTF_8.name(), settings.getCharset());
  }

  @Test
  public void customNewlineTest() throws Exception {
    UploadService service = new UploadService();
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    CsvFileDescriptor descriptor = new CsvFileDescriptor(CUSTOM_NEWLINE_CSV_FILE);
    CSVParser parser = new CSVParser(new ResourceFileStorage(), descriptor);
    descriptor.setSettings(parser.tryGetCsvSettings());
    descriptor.getSettings().setUseHeaders(false);
    descriptor.setColumns(service.tryParseColumns(parser));
    parser.parse().forEachRemaining(x -> {
      log.info(x.toString());
      result.add(x);
    });
    log.info("Got {} rows", result.size());
    assertEquals(7, result.size());
  }

  @Test
  public void parseTest() throws Exception {
    UploadService service = new UploadService();
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    CsvFileDescriptor descriptor = new CsvFileDescriptor(CSV_FILE);
    Parser parser = new CSVParser(new ResourceFileStorage(), descriptor);
    descriptor.setColumns(service.tryParseColumns(parser));
    parser.parse().forEachRemaining(x -> {
      log.info(x.toString());
      result.add(x);
    });
    assertTrue(DeepEquals.deepEquals(result, objects));
  }

  @Test
  public void detectSkipFromBottomTest() throws Exception {
    URL url = getClass().getClassLoader().getResource(CSV_FILE_WITH_TOTALS);
    if(url == null){
      Assert.fail("No such file: " + CSV_FILE_WITH_TOTALS);
    }
    BufferingInputStream bis = InputStreamUtils.buffering(url.openStream());
    IOUtils.copy(bis, new NullOutputStream());

    CsvFileDescriptor fileDescriptor = new CsvFileDescriptor(CSV_FILE_WITH_TOTALS);
    fileDescriptor.getOptions().put("LAST_BUFFERED_CONTENTS", bis.getBufferContents());

    CSVParser parser = new CSVParser(new ResourceFileStorage(), fileDescriptor);
    fileDescriptor.setSettings(parser.tryGetCsvSettings());

    Assert.assertTrue(4 == fileDescriptor.getSettings().getSkipFromBottom());

    RecordIterator it = parser.parse();
    List<Map<AbstractParsedColumn, Object>> result = Lists.newArrayList(it);
    Assert.assertEquals(998, result.size());
  }

  @Test
  public void nullStringTest() throws Exception {
    UploadService service = new UploadService();
    List<Map<AbstractParsedColumn, Object>> result = new ArrayList<>();
    CsvFileDescriptor descriptor = new CsvFileDescriptor(CSV_FILE_WITH_NULLS);
    Parser parser = new CSVParser(new ResourceFileStorage(), descriptor);
    descriptor.setColumns(service.tryParseColumns(parser));
    parser.parse().forEachRemaining(x -> {
      log.info(x.toString());
      result.add(x);
    });
    Assert.assertTrue(DeepEquals.deepEquals(result, csvWithNulls));
  }

}
