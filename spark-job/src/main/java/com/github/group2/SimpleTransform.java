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


}