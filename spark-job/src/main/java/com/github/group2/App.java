package com.github.group2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class App {
	public static void main(String[] args) {
		SimpleTransform simple = new SimpleTransform();

		simple.schema();

		// simple.sqlstuff();
		// simple.sql_peopleType();
		// simple.sql_countryRoom();
		// simple.sql_countryAdr();
		Dataset<Row> temp=simple.sql_topTenCountry();
		IO.datasetToCsv(temp,"topTen.csv");
	  
		// simple.compareHotelTypeAndMonth();
		// simple.compareAvgLeadTimeOfHotelType();
		// simple.compareAvgAdrOfRoomType();
		// simple.compareAvgAdrofHotel();

		// simple.monthlyAnalyses();
		// simple.summarizeCountries();
		// simple.cancellationAnalyses();

		// simple.countRepeatedGuestVSHotel();
		// simple.countReservedIsAssignedVSRoomType();
		// simple.countReservedIsAssignedVSMonth();
		// simple.countReservedIsAssignedVSYear();
		// simple.countReservedIsAssignedVSDay();
	}
	
}
