package com.dataparse.server.tool;

import org.junit.Test;


public class ToolIntegrationTest {
  
  @Test
  public void testCleanPostgresDatabase() throws Exception {
    ServerTool.postgresDropDB("jdbc:postgresql://localhost:5432/dataparse_test", "postgres", "postgres");
  }
  
  
  @Test
  public void testDeleteElasticsearchIndices() throws Exception {
    ServerTool.esDropIndices("localhost", 9200);
  }
}
