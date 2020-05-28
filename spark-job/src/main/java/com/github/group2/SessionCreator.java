package com.github.group2;

import org.apache.spark.sql.SparkSession;

public class SessionCreator {
  private static SparkSession spark;
  private static SessionCreator instance;
  

  private SessionCreator() {
    // create SparkSession
    if (spark == null) {												   
      spark = new SparkSession.Builder().appName("EZ APP").getOrCreate();
      spark.sparkContext().setLogLevel("WARN");
      spark.sparkContext().hadoopConfiguration().addResource("conf.xml");
      spark.sparkContext().hadoopConfiguration().set("fs.s3.canned.acl", "PublicReadWrite");
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
