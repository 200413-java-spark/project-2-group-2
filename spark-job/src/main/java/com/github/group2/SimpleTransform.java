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


  void compareHotelTypeAndMonth() {
    // Correlation between hotel_type and arrival_date_month             
    spark.sql("SELECT hotel, arrival_date_month, COUNT(hotel) FROM bookings group by hotel, arrival_date_month order by arrival_date_month").show(30);
       
  }

  void compareAvgLeadTimeOfHotelType() {
    // Correlation between lead_time and hotel_type
    spark.sql("SELECT hotel, COUNT(hotel), AVG(lead_time) FROM bookings group by hotel").show();
    
  }

  void compareAvgAdrOfRoomType() {
    spark.sql("SELECT assigned_room_type, AVG(adr) FROM bookings group by assigned_room_type").show();
  }
  







}