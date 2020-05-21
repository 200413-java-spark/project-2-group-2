package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
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

      
        
    }
    
    
}
