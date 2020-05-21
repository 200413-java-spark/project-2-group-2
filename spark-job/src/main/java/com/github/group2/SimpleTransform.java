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

  void sql_peopleType()
  {
    
    spark.sql("SELECT adults,children,babies,COUNT(*) as Total, ROUND(COUNT(*)*100/"+ds.count()+",3) as Percentage from bookings where group by adults,children,babies ORDER BY Total DESC").show(100);
  }
  void sql_countryRoom()
  {
    spark.sql("SELECT country,reserved_room_type,COUNT(*) as Total,ROUND(COUNT(*)*100/"+ds.count()+",3) as Percentage from bookings group by country,reserved_room_type ORDER BY country,reserved_room_type ASC").show(800);
  }

}