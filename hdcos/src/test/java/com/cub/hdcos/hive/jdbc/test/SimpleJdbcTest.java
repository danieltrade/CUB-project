package com.cub.hdcos.hive.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleJdbcTest {

	public static void main(String[] args) {
		Connection con = null;
		Statement sql = null;
		ResultSet res = null;
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			//con = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/default", "hive", "cloudera");
			con = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/default", "", "");
			sql = con.createStatement();
			res = sql.executeQuery("SELECT * FROM aaafn1");
			while (res.next()) {
				String id = res.getString(1);
				String nameEnocde = res.getString("name_encode");
				String age = res.getString(3);
				System.out.println(id + " , " + nameEnocde + " , " + age);
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(res != null) {
					res.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(sql != null) {
					sql.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(con != null) {
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
