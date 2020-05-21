package com.github.group2;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Moo {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark =
                new SparkSession.Builder().appName("EZ APP").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.sparkContext().hadoopConfiguration().addResource("conf.xml");

        Dataset<Row> ds = spark.read().option("inferSchema", true).option("header", true)
                .csv("s3a://revature-200413-project2-group2/hotel_bookings.csv").cache();
        /*
         * ds.printSchema(); ds.select("reservation_status_date").show(5); ds = ds.withColumn("ID",
         * functions.monotonically_increasing_id()); ds.createTempView("bookings");
         * spark.sql("SELECT ID, hotel, adults, children, reservation_status_date FROM bookings").
         * show(10);
         */
        System.out.println(Arrays.asList(ds.columns()).stream().collect(Collectors.joining(", ")));

        // some stats
        ds.select("hotel", "is_canceled", "lead_time", "arrival_date_month", "adr").describe().show();

        // monthly stuff
        ds.groupBy("arrival_date_month")
                .agg(count(lit(1)).alias("count"), avg("adr"))
                .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
        ds.groupBy("arrival_date_month", "hotel")
                .agg(count(lit(1)).alias("count"), avg("adr"))
                .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
        ds.groupBy("arrival_date_month", "is_canceled")
                .agg(count(lit(1)).alias("count"), avg("adr"))
                .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
        
    
        // countries
        ds.groupBy("country").count().withColumn("percentage", format_number(col("count").divide(ds.count()).multiply(100), 2)).sort(desc("count")).show();

        // cancellation
        ds.groupBy("is_canceled").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr")).show();
        ds.groupBy("hotel").avg("adr").show();
        ds.groupBy("is_canceled", "hotel").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr")).show();
    }
}
