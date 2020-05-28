package com.github.group2;

import com.github.group2.transfer.S3Transfer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DbTransfer {
  public static void main(String[] args) {
    try (InputStream input =
        S3Transfer.class.getClassLoader().getResourceAsStream("app.properties")) {
      Properties prop = new Properties(System.getProperties());
      prop.load(input);
      System.setProperties(prop);
    } catch (IOException e) {
      e.printStackTrace();
    }

    S3Transfer xfer = new S3Transfer();
    xfer.transferCancellationAnalyses();
    xfer.transferCancellationAnalysesOnHotel();
    xfer.transferCompareAvgAdrofHotel();
    xfer.transferCompareAvgAdrOfRoomType();
    xfer.transferCompareAvgLeadTimeOfHotelType();
    xfer.transferCompareHotelTypeAndMonth();
    xfer.transferCountRepeatedGuestVSHotel();
    xfer.transferCountReservedIsAssignedVSDay();
    xfer.transferCountReservedIsAssignedVSMonth();
    xfer.transferCountReservedIsAssignedVSRoomType();
    xfer.transferCountReservedIsAssignedVSYear();
    xfer.transferCountryVsAdr();
    xfer.transferCountryVsRevenue();
    xfer.transferCountryVsRoom();
    xfer.transferMonthlyAnalyses();
    xfer.transferMonthlyAnalysesOnHotel();
    xfer.transferMonthlyAnalysesOnIsCanceled();
    xfer.transferPeopleVsBooking();
    xfer.transferSummarizeCountries();
  }
}