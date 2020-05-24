package com.github.group2;

public class App {
	public static void main(String[] args) {
		SimpleTransform simple = new SimpleTransform();

		simple.peopleVsBooking();
		simple.countryVsRoom();
		simple.countryVsAdr();
		simple.countryVsRevenue();

		simple.summarizeCountries();

		simple.compareHotelTypeAndMonth();
		simple.compareAvgLeadTimeOfHotelType();
		simple.compareAvgAdrOfRoomType();
		simple.compareAvgAdrofHotel();

		simple.writeMonthlyAnalyses();

		simple.cancellationAnalyses1();
		simple.cancellationAnalyses2();

		simple.countRepeatedGuestVSHotel();
		simple.countReservedIsAssignedVSRoomType();
		simple.countReservedIsAssignedVSMonth();
		simple.countReservedIsAssignedVSYear();
		simple.countReservedIsAssignedVSDay();

	}
}
