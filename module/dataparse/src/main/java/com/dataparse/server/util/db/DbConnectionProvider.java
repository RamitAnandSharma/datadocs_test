package com.dataparse.server.util.db;

import com.dataparse.server.service.db.*;

import java.sql.*;


public interface DbConnectionProvider {

   Connection getConnection(ConnectionParams params);
   Connection getConnection(ConnectionParams params, Boolean streaming);

}
