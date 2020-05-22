package com.github.group2;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Moo {

  SparkSession spark = SessionCreator.getInstance().getSession();
  Dataset<Row> ds = DatasetCreator.getInstance(spark).getDataset();

  // System.out.println(Arrays.asList(ds.columns()).stream().collect(Collectors.joining(", ")));
  // some stats
  // ds.select("hotel", "is_canceled", "lead_time", "arrival_date_month", "adr").describe().show();

void mooAnalyze() {
    // monthly stuff
    ds.groupBy("arrival_date_month").agg(count(lit(1)).alias("count"), avg("adr"))
        .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
    ds.groupBy("arrival_date_month", "hotel").agg(count(lit(1)).alias("count"), avg("adr"))
        .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);
    ds.groupBy("arrival_date_month", "is_canceled").agg(count(lit(1)).alias("count"), avg("adr"))
        .sort(month(to_date(ds.col("arrival_date_month"), "MMMMM"))).show(24);


    // countries
    ds.groupBy("country").count()
        .withColumn("percentage", format_number(col("count").divide(ds.count()).multiply(100), 2))
        .sort(desc("count")).show();

    // cancellation
    ds.groupBy("is_canceled").agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr"))
        .show();
    ds.groupBy("hotel").avg("adr").show();
    ds.groupBy("is_canceled", "hotel")
        .agg(count(lit(1)).alias("count"), avg("lead_time"), avg("adr")).show();
  }

}
