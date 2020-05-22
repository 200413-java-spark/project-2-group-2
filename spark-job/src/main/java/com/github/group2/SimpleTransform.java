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
  
  void countRepeatedGuestVSHotel() {
		// Count of repeated guests vs type of hotel
		spark.sql("Select hotel, " + "count(*) AS total, "
				+ "Count(case when is_repeated_guest = 1 then hotel end) as CountofRepeatedGuests, "
				+ "Count(case when is_repeated_guest = 0 then hotel end) as CountofNonRepeatedGuests "
				+ "from bookings group by hotel").show();
  }
  
  void countReservedIsAssignedVSRoomType() {
		// count of where room reserved was the room assigned
		spark.sql("Select reserved_room_type, "
				+ "Count(case when assigned_room_type = reserved_room_type then reserved_room_type end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then reserved_room_type end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by reserved_room_type").show();
  }
  
  void countReservedIsAssignedVSMonth() {
		// count of where room reserved was the room assigned based on month
		spark.sql("Select arrival_date_month, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_month ").show();
  }
  
  void countReservedIsAssignedVSYear() {
	// count of where room reserved was the room assigned based on year
	  	spark.sql("Select arrival_date_year, "
	  			+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_year end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_year end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_year "
				+ "Order By arrival_date_year ASC").show();
  }
  void countReservedIsAssignedVSDay() {
	// count of where room reserved was the room assigned based on day of month
	  	spark.sql("Select arrival_date_day_of_month, "
	  			+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_day_of_month "
				+ "Order By arrival_date_day_of_month ASC").show(31);
  }
}