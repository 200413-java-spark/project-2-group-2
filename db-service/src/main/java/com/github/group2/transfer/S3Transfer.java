package com.github.group2.transfer;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class S3Transfer {
  RDSDataSource ds = RDSDataSource.getInstance();
  String bucketName = "revature-200413-project2-group2";
  String prefix = "JeffsResults/";

  public void transfer(String analysisAspect, String columns) {
    String useExt = "CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE";
    String dropSql = "DROP TABLE IF EXISTS " + analysisAspect;
    String createSql = "CREATE TABLE " + analysisAspect + " " + columns;
    String transferSql = "SELECT aws_s3.table_import_from_s3('" + analysisAspect
        + "', '', '(format csv, header true)', '" + bucketName + "', '" + prefix + analysisAspect
        + ".csv', 'us-west-1')";
    try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
      conn.setAutoCommit(false);
      stmt.execute(useExt);
      conn.commit();
      stmt.execute(dropSql);
      stmt.execute(createSql);
      stmt.execute(transferSql);
      conn.commit();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void transferCancellationAnalyses() {
    transfer("cancellationAnalyses",
        "(is_canceled BOOLEAN, count INTEGER, avg_lead_time DECIMAL, avg_adr DECIMAL)");
  }

  public void transferCancellationAnalysesOnHotel() {
    transfer("cancellationAnalysesOnHotel",
        "(is_canceled BOOLEAN, hotel VARCHAR, count INTEGER, avg_lead_time DECIMAL, avg_adr DECIMAL)");
  }

  public void transferCompareAvgAdrofHotel() {
    transfer("compareAvgAdrofHotel", "(hotel VARCHAR, AVG_ADR DECIMAL)");
  }

  public void transferCompareAvgAdrOfRoomType() {
    transfer("compareAvgAdrOfRoomType", "(assigned_room_type CHAR, avg_adr DECIMAL)");
  }

  public void transferCompareAvgLeadTimeOfHotelType() {
    transfer("compareAvgLeadTimeOfHotelType",
        "(hotel VARCHAR, count INTEGER, avg_lead_time DECIMAL)");
  }

  public void transferCompareHotelTypeAndMonth() {
    transfer("compareHotelTypeAndMonth",
        "(hotel VARCHAR, arrival_date_month VARCHAR, count INTEGER)");
  }

  public void transferCountRepeatedGuestVSHotel() {
    transfer("countRepeatedGuestVSHotel",
        "(hotel VARCHAR, total INTEGER, CountofRepeatedGuests INTEGER, CountofNonRepeatedGuests INTEGER)");
  }

  public void transferCountReservedIsAssignedVSDay() {
    transfer("countReservedIsAssignedVSDay",
        "(arrival_date_day_of_month INTEGER, CountWhereReservedIsAssigned INTEGER, CountWhereReservedIsNotAssigned INTEGER)");
  }

  public void transferCountReservedIsAssignedVSMonth() {
    transfer("countReservedIsAssignedVSMonth",
        "(arrival_date_month VARCHAR, CountWhereReservedIsAssigned INTEGER, CountWhereReservedIsNotAssigned INTEGER)");
  }

  public void transferCountReservedIsAssignedVSRoomType() {
    transfer("countReservedIsAssignedVSRoomType",
        "(reserved_room_type CHAR, CountWhereReservedIsAssigned INTEGER, CountWhereReservedIsNotAssigned INTEGER)");
  }

  public void transferCountReservedIsAssignedVSYear() {
    transfer("countReservedIsAssignedVSYear",
        "(arrival_date_year INTEGER, CountWhereReservedIsAssigned INTEGER, CountWhereReservedIsNotAssigned INTEGER)");
  }

  public void transferCountryVsAdr() {
    transfer("countryVsAdr",
        "(Country CHAR(4), Number_of_Booking INTEGER, Average_Daily_Rate DECIMAL)");
  }

  public void transferCountryVsRevenue() {
    transfer("countryVsRevenue",
        "(Country CHAR(4), Total_Revenue DECIMAL, Count INTEGER, Percentage DECIMAL)");
  }

  public void transferCountryVsRoom() {
    transfer("countryVsRoom",
        "(Country CHAR(4), Room_Type CHAR, Total INTEGER, Percentage DECIMAL)");
  }

  public void transferMonthlyAnalyses() {
    transfer("monthlyAnalyses", "(arrival_date_month VARCHAR, count INTEGER, avg_adr DECIMAL)");
  }

  public void transferMonthlyAnalysesOnHotel() {
    transfer("monthlyAnalysesOnHotel",
        "(arrival_date_month VARCHAR, hotel VARCHAR,count INTEGER, avg_adr DECIMAL)");
  }

  public void transferMonthlyAnalysesOnIsCanceled() {
    transfer("monthlyAnalysesOnIsCanceled",
        "(arrival_date_month VARCHAR,is_canceled BOOLEAN, count INTEGER, avg_adr DECIMAL)");
  }

  public void transferPeopleVsBooking() {
    transfer("peopleVsBooking", "(Adults INTEGER, Children INTEGER, Babies INTEGER, Total INTEGER, Percentage DECIMAL)");
  }

  public void transferSummarizeCountries() {
    transfer("summarizeCountries", "(Country CHAR(4), count INTEGER, percentage DECIMAL)");
  }

}

