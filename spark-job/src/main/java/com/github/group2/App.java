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

        long num=ds.count();
        JavaPairRDD <String,Integer> pair=method_1(ds);
        for(Tuple2<String,Integer> s:pair.collect())
        {
        	System.out.printf(s+"  -> "+"%.4f",(s._2*100.0/num));
        	System.out.println("%");
        }
        
    }
    
    public static JavaPairRDD<String,Integer> method_1(Dataset<Row> data)
	{
    	JavaRDD<Row> temp=data.javaRDD();
		JavaPairRDD<String,Integer> result= temp.mapToPair(new PairFunction<Row,String,Integer>()
				{
					@Override
					public Tuple2<String,Integer> call (Row input) throws Exception{
						return new Tuple2<String,Integer>((input.get(9)+", "+input.get(10)+", "+input.get(11)),1);
					}
				}
				);
		result=result.reduceByKey((x,y)->x+y);
		return result;
	}
}
