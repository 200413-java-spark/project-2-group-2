package com.github.group2;

import org.apache.spark.sql.SparkSession;

public class SessionCreator {
  private static SparkSession spark;
  private static SessionCreator instance;
  

  private SessionCreator() {
    // create SparkSession
    if (spark == null) {												   
      spark = new SparkSession.Builder().appName("EZ APP").master("local").getOrCreate();
      spark.sparkContext().setLogLevel("WARN");
      spark.sparkContext().hadoopConfiguration().addResource("conf.xml");
    }
  }

  public static SessionCreator getInstance() {
    if (instance == null) {
      instance = new SessionCreator();
    }
    return instance;
  }

  public SparkSession getSession() {
    return spark;
  }
}
