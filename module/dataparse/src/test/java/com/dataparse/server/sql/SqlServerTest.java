package com.dataparse.server.sql;

import com.dataparse.server.service.parser.CSVParser;
import com.dataparse.server.service.parser.Parser;
import com.dataparse.server.service.parser.column.AbstractParsedColumn;
import com.dataparse.server.service.parser.column.ParsedColumnFactory;
import com.dataparse.server.service.parser.iterator.RecordIterator;
import com.dataparse.server.service.parser.type.ColumnInfo;
import com.dataparse.server.service.storage.LocalFileStorage;
import com.dataparse.server.service.upload.CsvFileDescriptor;
import com.dataparse.server.service.upload.UploadService;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

public class SqlServerTest {

  @Test
  public void simpleQueryTest() {
    long start = System.currentTimeMillis();
    try {
      Driver driver = new com.mysql.jdbc.Driver();
      DriverManager.registerDriver(driver);
      String url = "jdbc:sqlserver://localhost;database=testdata";


      CsvFileDescriptor descriptor = new CsvFileDescriptor();
      descriptor.setPath("copy_1M.csv");

      LocalFileStorage storage = new LocalFileStorage();
      storage.init();
      Parser parser = new CSVParser(storage, descriptor);
      UploadService uploadService = new UploadService();
      List<ColumnInfo> columns = uploadService.tryParseColumns(parser);
      descriptor.setColumns(columns);

      int count = 0, batchSize = 100;
      try(RecordIterator iterator = parser.parse();
          Connection conn = DriverManager.getConnection(url, "SA", "123QWEasd");
          PreparedStatement st = conn.prepareStatement("INSERT INTO cm1 " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")){
        while(iterator.hasNext()){
          Map<AbstractParsedColumn, Object> o = iterator.next();

          int i = 0;
          for(ColumnInfo column: columns){
            i++;
            Object value = o.get(ParsedColumnFactory.getByColumnInfo(column));
            st.setObject(i, value);
          }
          st.addBatch();
          if(++count % batchSize == 0) {
            st.executeBatch();
          }
        }
        st.executeBatch();
      }
      System.out.println(columns);
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println((System.currentTimeMillis() - start));
  }
}
