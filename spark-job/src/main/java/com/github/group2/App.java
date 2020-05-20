package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.AnalysisException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark =
                new SparkSession.Builder().appName("EZ APP").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3-us-west-1.amazonaws.com");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "AKIAQ5ZAO5DQKCVMRH7F");
		spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "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");
        
        Dataset<Row> ds = spark.read().option("inferSchema", true).option("header", true)
                .csv("s3a://revature-200413-project2-group2/hotel_bookings.csv").cache();
        ds.printSchema();
        ds.select("reservation_status_date").show(5);
        ds = ds.withColumn("ID", functions.monotonically_increasing_id());
        ds.createTempView("bookings");
        spark.sql("SELECT ID, hotel, adults, children, reservation_status_date FROM bookings").show(10);    
    }
}
