package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App {
	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = new SparkSession.Builder().appName("EZ APP").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		spark.sparkContext().hadoopConfiguration().addResource("conf.xml");
		Dataset<Row> ds = spark.read().option("inferSchema", true).option("header", true)
				.csv("s3a://revature-200413-project2-group2/hotel_bookings.csv").cache();
		ds.printSchema();
		ds.select("reservation_status_date").show(5);
		ds = ds.withColumn("ID", functions.monotonically_increasing_id());
		ds.createTempView("bookings");
		spark.sql("SELECT ID, hotel, adults, children, reservation_status_date FROM bookings").show(10);

		// Count of repeated guests vs type of hotel
		spark.sql("Select hotel, " + "count(*) AS total, "
				+ "Count(case when is_repeated_guest = 1 then hotel end) as CountofRepeatedGuests, "
				+ "Count(case when is_repeated_guest = 0 then hotel end) as CountofNonRepeatedGuests "
				+ "from bookings group by hotel").show();

		// count of where room reserved was the room assigned
		spark.sql("Select reserved_room_type, "
				+ "Count(case when assigned_room_type = reserved_room_type then reserved_room_type end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then reserved_room_type end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by reserved_room_type").show();

		// count of where room reserved was the room assigned based on month
		spark.sql("Select arrival_date_month, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_month").show();
	}
}
