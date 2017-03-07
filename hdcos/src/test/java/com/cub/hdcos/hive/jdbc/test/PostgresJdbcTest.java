package com.cub.hdcos.hive.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresJdbcTest {
	
	public static void main(String[] args) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://88.8.141.180:5432/cubtestdb", "cubapp","123456");
			//connection = DriverManager.getConnection("jdbc:postgresql://88.252.199.62:5432/postgres", "postgres","1qaz0821");
			System.out.println("connection.isClosed() : " + connection.isClosed());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}
