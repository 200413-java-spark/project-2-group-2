package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DatasetCreator {
  private static Dataset<Row> ds;
  private static DatasetCreator instance;

  private DatasetCreator() {
  }

  private DatasetCreator(SparkSession spark) {
	  //create dataset
    if (ds == null) {
      ds = spark.read().option("inferSchema", true).option("header", true)
          .csv("s3a://revature-200413-project2-group2/hotel_bookings.csv").cache();
      try {
        ds.createTempView("bookings");
      } catch (AnalysisException e) {
        System.err.println(e.getMessage());
      }
    }
  }

  public static DatasetCreator getInstance(SparkSession spark) {
    if (instance == null) {
      instance = new DatasetCreator(spark);
    }
    return instance;
  }

  public Dataset<Row> getDataset() {
    return ds;
  }

}
