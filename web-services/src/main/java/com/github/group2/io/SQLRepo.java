package com.github.group2.io;

import java.sql.Statement;
import java.util.ArrayList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLRepo {

	public ArrayList<ArrayList<String>> readAll(String tableName) {
		// String[] columns = new String[numColumns];
		RDSDataSource rs = RDSDataSource.getInstance();

		ArrayList<ArrayList<String>> returnList = new ArrayList<ArrayList<String>>();

		try (Connection conn = rs.getConnection();) {
			// read statement
			Statement stmt = conn.createStatement();
			ResultSet stmtRs = stmt.executeQuery("Select * from " + tableName);
			int numColumns = stmtRs.getMetaData().getColumnCount();

			// grab header from sql table
			ArrayList<String> headerList = new ArrayList<String>();
			for (int i = 1; i <= numColumns; i++) {
				headerList.add(stmtRs.getMetaData().getColumnName(i));
			}
			returnList.add(headerList);

			// grab data in table
			while (stmtRs.next()) {
				ArrayList<String> testList = new ArrayList<String>();
				for (int i = 1; i <= numColumns; i++) {
					testList.add(stmtRs.getString(i));
				}
				returnList.add(testList);
			}

		} catch (SQLException ex) {
			System.err.println(ex.getMessage());
		}
		return returnList;
	}

}
