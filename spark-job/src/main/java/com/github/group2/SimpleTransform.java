package com.github.group2;

import static org.apache.spark.sql.functions.*;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class SimpleTransform {
	private SparkSession spark;
	private Dataset<Row> ds;

	public SimpleTransform() {
		spark = SessionCreator.getInstance().getSession();
		ds = DatasetCreator.getInstance(spark).getDataset();
	}

	// probably not needed
	public SimpleTransform(SparkSession spark, Dataset<Row> ds) {
		this.spark = spark;
		this.ds = ds;
	}

	void schema() {
		ds.printSchema();
	}

	void sqlstuff() {
		spark.sql("SELECT hotel FROM bookings").show(10);
	}

	void sql_peopleType() {
		spark.sql(
				"SELECT adults as Adults,children as Children,babies as Babies,COUNT(*) as Total, ROUND(COUNT(*)*100/"
						+ ds.count()
						+ ",3) as Percentage from bookings where group by adults,children,babies ORDER BY Total DESC")
				.show(100);
	}

	void sql_countryRoom() {
		spark.sql(
				"SELECT country as Country,reserved_room_type as Room_Type,COUNT(*) as Total,ROUND(COUNT(*)*100/"
						+ ds.count()
						+ ",3) as Percentage from bookings group by country,reserved_room_type ORDER BY country,reserved_room_type ASC")
				.show(800);
	}

	void sql_countryAdr() {
		spark.sql(
				"SELECT country as Country,COUNT(adr)as Number_of_Booking ,ROUND(SUM(adr)/COUNT(adr),2)as Average_Daily_Rate FROM bookings GROUP BY country ORDER BY country ASC ")
				.show(200);
	}

	void sql_topTenCountry() {
		spark.sql("SELECT country as Country,ROUND(SUM(adr),2) as Total_Revenue, "
				+ "Count(country) as Count, " + "ROUND(COUNT(country)/" + ds.count()
				+ " * 100,2) as Percentage " + "FROM bookings GROUP BY country ORDER BY Total_Revenue DESC")
				.show(10);

	}

	void summarizeCountries() {
		// countries
		ds.groupBy("country").count()
				.withColumn("percentage", format_number(col("count").divide(ds.count()).multiply(100), 2))
				.sort(desc("count")).show();
	}

	void compareHotelTypeAndMonth() {
		// Correlation between hotel_type and arrival_date_month
		spark.sql(
				"SELECT hotel, arrival_date_month, COUNT(hotel) FROM bookings group by hotel, arrival_date_month order by arrival_date_month")
				.show(30);

	}

	void compareAvgLeadTimeOfHotelType() {
		// Correlation between lead_time and hotel_type
		spark.sql("SELECT hotel, COUNT(hotel), AVG(lead_time) FROM bookings group by hotel").show();

	}

	void compareAvgAdrOfRoomType() {
		// Correlation between adr and room type
		spark.sql("SELECT assigned_room_type, AVG(adr) FROM bookings group by assigned_room_type")
				.show();
	}

	void compareAvgAdrofHotel() {
		// Correlation between adr and hotel
		spark.sql("SELECT hotel, AVG(adr) FROM bookings group by hotel").show();

	}

	void monthlyAnalyses() {
		// monthly stuff
		ds.groupBy("arrival_date_month").agg(count(lit(1)).alias("count"), avg("adr"))
				.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
		ds.groupBy("arrival_date_month", "hotel").agg(count(lit(1)).alias("count"), avg("adr"))
				.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("hotel")).show(24);
		ds.groupBy("arrival_date_month", "is_canceled").agg(count(lit(1)).alias("count"), avg("adr"))
				.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("is_canceled"))
				.show(24);
	}

	void writeMonthlyAnalyses() {
		ds.groupBy("arrival_date_month", "is_canceled").agg(count(lit(1)).alias("count"), avg("adr"))
				.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("is_canceled"))
				.coalesce(1).write().format("csv").option("header", true)
				.save("s3a://revature-200413-project2-group2/results/");
	}

	void cancellationAnalyses() {
		// cancellation
		ds.groupBy("is_canceled").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr"))
				.show();
		ds.groupBy("is_canceled", "hotel")
				.agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr")).show();
	}

	void writeCancellationAnalyses() {
		// URI bucket = URI.create("s3a://")
		ds.groupBy("is_canceled", "hotel")
				.agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr")).coalesce(1).write()
				.format("csv").option("header", true).mode("overwrite")
				// .save("s3a://home/jimey/revature/project2/results/renametest/");
				.save("s3a://revature-200413-project2-group2/results/renametest/");

		try {
			FileSystem fs = FileSystem.get(new URI("s3a://revature-200413-project2-group2/"),spark.sparkContext().hadoopConfiguration());
			// FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
			RemoteIterator<LocatedFileStatus> status = fs.listFiles(new Path("s3a://revature-200413-project2-group2/results/renametest/"), true);
			String partPath = "";
			while(status.hasNext()) {
				String current = status.next().getPath().getName();
				System.out.println(current);
				if (current.contains("part")) {
					partPath = current;
					System.out.println(partPath);
				}
			}
			fs.rename(
					//new Path(new URI("s3a://revature-200413-project2-group2/results/renametest/part*.csv")),
					new Path(partPath),
					new Path(new URI("s3a://revature-200413-project2-group2/results/renametest/test.csv")));
		} catch (IOException | IllegalArgumentException | URISyntaxException e) {
			e.printStackTrace();
		}
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
				+ "From bookings Group by arrival_date_year " + "Order By arrival_date_year ASC").show();
	}

	void countReservedIsAssignedVSDay() {
		// count of where room reserved was the room assigned based on day of month
		spark.sql("Select arrival_date_day_of_month, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_day_of_month "
				+ "Order By arrival_date_day_of_month ASC").show(31);
	}

	void writeCountReservedIsAssignedVSDay() {
		// count of where room reserved was the room assigned based on day of month
		// spark.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version",
		// "2");
		spark.sql("Select arrival_date_day_of_month, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_day_of_month "
				+ "Order By arrival_date_day_of_month ASC").coalesce(1).write().format("csv")
				.option("header", true).save("s3a://revature-200413-project2-group2/results/test.csv");
	}
}

