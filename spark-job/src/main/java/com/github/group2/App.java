package com.github.group2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class App {
    public static void main(String[] args) {
        SimpleTransform simple = new SimpleTransform();


        simple.schema();
		simple.sqlstuff();
    	simple.sql_peopleType();
        simple.sql_countryRoom();
        simple.sql_countryAdr();
        simple.sql_topTenCountry();

        simple.compareHotelTypeAndMonth();
        simple.compareAvgLeadTimeOfHotelType();
        simple.compareAvgAdrOfRoomType();
        simple.compareAvgAdrofMonth();
        simple.compareAvgAdrofHotel();

        Moo moo = new Moo();
        moo.mooAnalyze();

    }


}
