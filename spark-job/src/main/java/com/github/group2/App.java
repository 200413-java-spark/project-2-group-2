package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark =
                new SparkSession.Builder().appName("EZ APP").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.sparkContext().hadoopConfiguration().addResource("conf.xml");
        Dataset<Row> ds = spark.read().option("inferSchema", true).option("header", true)
                .csv("s3a://revature-200413-project2-group2/hotel_bookings.csv").cache();
        ds.printSchema();
        ds.select("reservation_status_date").show(5);
        ds = ds.withColumn("ID", functions.monotonically_increasing_id());
        ds.createTempView("bookings");
        spark.sql("SELECT ID, hotel, adults, children, reservation_status_date FROM bookings")
             .show(10);



        // Correlation between hotel_type and arrival_date_month     
        spark.sql("SELECT hotel, arrival_date_month, COUNT(hotel) FROM bookings group by hotel, arrival_date_month order by arrival_date_month").show(30);
       
        // Correlation between lead_time and hotel_type
        spark.sql("SELECT hotel, COUNT(hotel), AVG(lead_time) FROM bookings group by hotel").show();

    }


}
