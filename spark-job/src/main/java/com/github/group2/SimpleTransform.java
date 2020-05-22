package com.github.group2;


import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

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
    spark.sql("SELECT adults as Adults,children as Children,babies as Babies,COUNT(*) as Total, ROUND(COUNT(*)*100/"+ds.count()+",3) as Percentage from bookings where group by adults,children,babies ORDER BY Total DESC").show(100);
  }
  void sql_countryRoom()
  {
    spark.sql("SELECT country as Country,reserved_room_type as Room_Type,COUNT(*) as Total,ROUND(COUNT(*)*100/"+ds.count()+",3) as Percentage from bookings group by country,reserved_room_type ORDER BY country,reserved_room_type ASC").show(800);
  }
  void sql_countryAdr()
  {
    spark.sql("SELECT country as Country,COUNT(adr)as Number_of_Booking ,ROUND(SUM(adr)/COUNT(adr),2)as Average_Daily_Rate FROM bookings GROUP BY country ORDER BY country ASC ").show(200);
  }
  void sql_topTenCountry()
  {
    spark.sql("SELECT country as Country,ROUND(SUM(adr),2) as Total_Revenue FROM bookings GROUP BY country ORDER BY Total_Revenue DESC").show(10);

  }


  void monthlyAnalyses() {
    // monthly stuff
    ds.groupBy("arrival_date_month").agg(count(lit(1)).alias("count"), avg("adr"))
        .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
    ds.groupBy("arrival_date_month", "hotel").agg(count(lit(1)).alias("count"), avg("adr"))
        .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("hotel")).show(24);
    ds.groupBy("arrival_date_month", "is_canceled").agg(count(lit(1)).alias("count"), avg("adr"))
        .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("is_canceled")).show(24);
  }

  void summarizeCountries() {
    // countries
    ds.groupBy("country").count()
        .withColumn("percentage", format_number(col("count").divide(ds.count()).multiply(100), 2))
        .sort(desc("count")).show();
  }

  void cancellationAnalyses() {
    // cancellation
    ds.groupBy("is_canceled").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr"))
        .show();
    ds.groupBy("hotel").avg("adr").show();
    ds.groupBy("is_canceled", "hotel")
        .agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr")).show();
  }

}