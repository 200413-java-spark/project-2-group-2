package com.github.group2.transfer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RDSDataSource {
  private static RDSDataSource instance;
  private String url;
  private String user;
  private String password;

  private RDSDataSource() {
    url = System.getProperty("db.url");
    user = System.getProperty("db.user");
    password = System.getProperty("db.password");
  }

  public static RDSDataSource getInstance() {
    if (instance == null) {
    	
      instance = new RDSDataSource();
    }
    return instance;
  }

  public Connection getConnection() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }
}
