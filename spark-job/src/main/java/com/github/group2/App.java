package com.github.group2;

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
		simple.compareAvgAdrofHotel();

		simple.monthlyAnalyses();
		simple.summarizeCountries();
		simple.cancellationAnalyses();

		simple.countRepeatedGuestVSHotel();
		simple.countReservedIsAssignedVSRoomType();
		simple.countReservedIsAssignedVSMonth();
		simple.countReservedIsAssignedVSYear();
		simple.countReservedIsAssignedVSDay();
	}
}
