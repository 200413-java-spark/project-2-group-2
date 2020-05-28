package com.github.group2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

import com.github.group2.io.SQLRepo;

public class Server {
	public static void main(String[] args) {

		getProperties();
		startTomcat();
	}

	private static void startTomcat() {
		Tomcat tomcat = new Tomcat();
		tomcat.setPort(8080);
		tomcat.getConnector();
		tomcat.setBaseDir(new File("target/tomcat/").getAbsolutePath());
		tomcat.addWebapp("/group2", new File("src/main/resources/").getAbsolutePath());
		tomcat.addServlet("/group2", "DisplayDatabaseServlet", new DisplayDatabaseServlet())
				.addMapping("/DisplayDatabase");

		try {
			tomcat.start();
			System.out.println("Server is running!");
		} catch (LifecycleException ex) {
			System.out.println("Server run failed!");
			System.err.println(ex.getMessage());
		}
	}

	private static void getProperties() {
		try (InputStream input = SQLRepo.class.getClassLoader().getResourceAsStream("app.properties")) {
			Properties prop = new Properties(System.getProperties());
			prop.load(input);
			System.setProperties(prop);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
