package com.github.group2;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.group2.io.SQLRepo;

public class DisplayDatabaseServlet extends HttpServlet{

	private static final long serialVersionUID = 1L;

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
	
		String value = req.getParameter("transformations");
		SQLRepo sqlRepo = new SQLRepo();
		System.out.println(req.getParameter("transformations") + " is being displayed");
		ArrayList<ArrayList<String>> test = sqlRepo.readAll(value);
		
		PrintWriter out = resp.getWriter();
		for(ArrayList<String> tests:test)
		{
			out.println(tests);
		}
	}

}
