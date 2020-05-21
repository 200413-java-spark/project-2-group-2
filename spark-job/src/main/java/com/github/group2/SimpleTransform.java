package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SimpleTransform {
  private SparkSession spark;
  private Dataset<Row> ds;

  public SimpleTransform() {
    spark = SessionCreator.getInstance().getSession();
    ds = DatasetCreator.getInstance(spark).getDataset();
  }
  
  // probably not needed
  public SimpleTransform (SparkSession spark, Dataset<Row> ds) {
    this.spark = spark;
    this.ds = ds;
  }
  
  void schema() {
    ds.printSchema();
  }
  
  void sqlstuff() {
    spark.sql("SELECT hotel FROM bookings").show(10);
  }
}