package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.function.*;

// TEST COMMENT: HELLO!


/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark =
                new SparkSession.Builder().appName("EZ APP").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        Dataset<Row> ds = spark.read().option("inferSchema", true).option("header", true)
                .csv("../hotel_bookings.csv").cache();
        ds.printSchema();
        ds.select("reservation_status_date").show(5);
        ds = ds.withColumn("ID", functions.monotonically_increasing_id());
        ds.createTempView("bookings");
        spark.sql("SELECT ID, hotel, adults, children, reservation_status_date FROM bookings").show(10);
    }

}
