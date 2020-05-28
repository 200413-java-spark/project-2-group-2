package com.github.group2;


public class App {
	public static void main(String[] args) {
		SimpleTransform simple = new SimpleTransform();
		simple.cancellationAnalyses();
		simple.cancellationAnalysesOnHotel();
		simple.compareAvgAdrOfRoomType();
		simple.compareAvgAdrofHotel();
		simple.compareAvgLeadTimeOfHotelType();
		simple.compareHotelTypeAndMonth();
		simple.countRepeatedGuestVSHotel();
		simple.countReservedIsAssignedVSDay();
		simple.countReservedIsAssignedVSMonth();
		simple.countReservedIsAssignedVSRoomType();
		simple.countReservedIsAssignedVSYear();
		simple.countryVsAdr();
		simple.countryVsRevenue();
		simple.countryVsRoom();
		simple.monthlyAnalyses();
		simple.monthlyAnalysesOnHotel();
		simple.monthlyAnalysesOnIsCanceled();
		simple.peopleVsBooking();
		simple.summarizeCountries();
		
	}
}
