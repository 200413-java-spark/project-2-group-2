package com.github.group2;

import static org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SimpleTransform {
	private SparkSession spark;
	private Dataset<Row> ds;
	private String savePath = "s3a://revature-200413-project2-group2/Results/";

	public SimpleTransform() {
		spark = SessionCreator.getInstance().getSession();
		ds = DatasetCreator.getInstance(spark).getDataset();
	}

	
	//Generate Adults,Children, Babies, number of specific combination of adult,children,and babies, and percentage of the combination of all booking.
	public Dataset<Row> peopleVsBooking() {
		Dataset<Row> result=spark.sql("SELECT adults as Adults,children as Children,babies as Babies,COUNT(*) as Total, ROUND(COUNT(*)*100/"
				+ ds.count()
				+ ",3) as Percentage from bookings where group by adults,children,babies ORDER BY Total DESC");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}
	//Generate Country, Room type, Number of each country booking, and percentage of each country booking with specify room type. 
	public Dataset<Row> countryVsRoom() {
		Dataset<Row> result=spark.sql("SELECT country as Country,reserved_room_type as Room_Type,COUNT(*) as Total,ROUND(COUNT(*)*100/"
				+ ds.count()
				+ ",3) as Percentage from bookings group by country,reserved_room_type ORDER BY country,reserved_room_type ASC");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}
	//Generate Country and Average daliy rate of each country and return Country name ,number of each country booking, and average of each country ADR

	public Dataset<Row> countryVsAdr() {
		Dataset<Row> result=spark.sql("SELECT country as Country,COUNT(adr)as Number_of_Booking ,ROUND(SUM(adr)/COUNT(adr),2)as Average_Daily_Rate FROM bookings GROUP BY country ORDER BY country ASC ");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}
	//Generate Country, Total_Revenue, Each country number of booking, and percentage of booking number.
	public Dataset<Row> countryVsRevenue() {
		Dataset<Row> result=spark.sql("SELECT country as Country,ROUND(SUM(adr),2) as Total_Revenue, " + "Count(country) as Count, "
				+ "ROUND(COUNT(country)/" + ds.count() + " * 100,2) as Percentage "
				+ "FROM bookings GROUP BY country ORDER BY Total_Revenue DESC");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> summarizeCountries() {
		// countries
		Dataset<Row> result=ds.groupBy("country").count().withColumn("percentage", format_number(col("count").divide(ds.count()).multiply(100), 2)).sort(desc("count"));
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> compareHotelTypeAndMonth() {
		// Correlation between hotel_type and arrival_date_month
		Dataset<Row> result=spark.sql("SELECT hotel, arrival_date_month, COUNT(hotel) FROM bookings group by hotel, arrival_date_month order by arrival_date_month");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> compareAvgLeadTimeOfHotelType() {
		// Correlation between lead_time and hotel_type
		Dataset<Row> result=spark.sql("SELECT hotel, COUNT(hotel), AVG(lead_time) FROM bookings group by hotel");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;

	}

	public Dataset<Row> compareAvgAdrOfRoomType() {
		// Correlation between adr and room type
		Dataset<Row> result=spark.sql("SELECT assigned_room_type, AVG(adr) FROM bookings group by assigned_room_type");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> compareAvgAdrofHotel() {
		// Correlation between adr and hotel
		Dataset<Row> result=spark.sql("SELECT hotel, AVG(adr) FROM bookings group by hotel");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> monthlyAnalyses() {
		// monthly stuff
		Dataset<Row> result=ds.groupBy("arrival_date_month").agg(count(lit(1)).alias("count"), avg("adr"))
				.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")));
			
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}
	
	public Dataset<Row> monthlyAnalysesOnHotel()
	{
		Dataset<Row> result=ds.groupBy("arrival_date_month", "hotel").agg(count(lit(1)).alias("count"), avg("adr"))
		.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("hotel"));
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> monthlyAnalysesOnIsCanceled() {
		Dataset<Row> result=ds.groupBy("arrival_date_month", "is_canceled").agg(count(lit(1)).alias("count"), avg("adr"))
				.sort(month(to_date(ds.col("arrival_date_month"), "MMMMM")), ds.col("is_canceled"));
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> cancellationAnalyses() {
		// cancellation
		Dataset<Row> result=ds.groupBy("is_canceled").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr"));
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> cancellationAnalysesOnHotel() {
		// cancellation with hotel
		Dataset<Row> result=ds.groupBy("is_canceled", "hotel").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr"));
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> countRepeatedGuestVSHotel() {
		// Count of repeated guests vs type of hotel
		Dataset<Row> result=spark.sql("Select hotel, " + "count(*) AS total, "
				+ "Count(case when is_repeated_guest = 1 then hotel end) as CountofRepeatedGuests, "
				+ "Count(case when is_repeated_guest = 0 then hotel end) as CountofNonRepeatedGuests "
				+ "from bookings group by hotel");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> countReservedIsAssignedVSRoomType() {
		// count of where room reserved was the room assigned
		Dataset<Row> result=spark.sql("Select reserved_room_type, "
				+ "Count(case when assigned_room_type = reserved_room_type then reserved_room_type end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then reserved_room_type end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by reserved_room_type");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> countReservedIsAssignedVSMonth() {
		// count of where room reserved was the room assigned based on month
		Dataset<Row> result=spark.sql("Select arrival_date_month, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_month "
				+ "Order By month(to_date(arrival_date_month, 'MMMM')) ASC");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> countReservedIsAssignedVSYear() {
		// count of where room reserved was the room assigned based on year
		Dataset<Row> result=spark.sql("Select arrival_date_year, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_year end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_year end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_year " + "Order By arrival_date_year ASC");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}

	public Dataset<Row> countReservedIsAssignedVSDay() {
		// count of where room reserved was the room assigned based on day of month
		Dataset<Row> result=spark.sql("Select arrival_date_day_of_month, "
				+ "Count(case when assigned_room_type == reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsAssigned, "
				+ "Count(case when assigned_room_type <> reserved_room_type then arrival_date_day_of_month end) as CountWhereReservedIsNotAssigned "
				+ "From bookings Group by arrival_date_day_of_month " + "Order By arrival_date_day_of_month ASC");
		result.coalesce(1).write().format("csv").option("header", true).mode("Append").save(this.savePath);
		rename(this.savePath, Thread.currentThread().getStackTrace()[1].getMethodName());
		return result;
	}
	
	void rename(String savePath, String name) {
		try {
			FileSystem fs = FileSystem.get(new URI(savePath), spark.sparkContext().hadoopConfiguration());
			checkDuplicate(savePath, name);		
			String partPath = fs.globStatus(new Path(savePath + "part*.csv"))[0].getPath().toString();
			fs.rename(new Path(partPath), new Path(savePath + name + ".csv"));
			fs.deleteOnExit(new Path(savePath + "_SUCCESS"));
		} catch (IOException | IllegalArgumentException | URISyntaxException e) {
			e.printStackTrace();
		}
	}

	public void checkDuplicate(String savePath,String name)
	{
		try
		{
			FileSystem fs = FileSystem.get(new URI(savePath), spark.sparkContext().hadoopConfiguration());
			String dup=fs.globStatus(new Path(savePath+name+".csv"))[0].getPath().toString();
			String temp[]=dup.split(savePath);
			if(temp[1].equals(name+".csv"))
			{
				fs.delete(new Path(dup),true);
				System.out.println(name + ".csv found.\nOverwriting "+name+".csv");
			}
			
		}
		catch (NullPointerException e)
		{
			System.out.println("No "+name +".csv found.\nCreating "+name+".csv");
		}
		catch( IOException | IllegalArgumentException | URISyntaxException e)
		{
			System.err.println(e.getMessage());
		}
		
	}
	
	public void display_dataset(Dataset<Row> data,int num)
	{
		data.show(num);
	}
}
