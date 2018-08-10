package com.dataparse.server.tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class ServerTool {
  static public void cleanAllWithDefaultConfig() {
    try { 
      postgresDropDB("jdbc:postgresql://localhost:5432/dataparse_test", "postgres", "postgres");
      esDropIndices("localhost", 9200);
    } catch(Throwable t) {
      throw new RuntimeException(t);
    }
  }
  
  
  static public void postgresDropDB(String uri, String owner, String password) throws Exception {
    Connection c = DriverManager.getConnection(uri, owner, password);
    execStatement(c, "DROP SCHEMA public CASCADE");
    execStatement(c, "CREATE SCHEMA public");
    execStatement(c, "GRANT ALL ON SCHEMA public TO postgres");
    execStatement(c, "GRANT ALL ON SCHEMA public TO public");
    c.close();
  }
  
  static void execStatement(Connection c, String sql) throws Exception {
    Statement statement = c.createStatement();
    statement.executeUpdate(sql);
    statement.close();
  }

  static public void esDropIndices(String host, int port) throws Exception {
    RestClient client = RestClient.builder(new HttpHost(host, port, "http")).build();
    client.performRequest("DELETE", "*");
  }
}
