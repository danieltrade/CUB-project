package com.cub.hdcos.hive.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HiveJdbcTest {
	
	Connection con = null;
	PreparedStatement stmt = null;
	//PreparedStatement
	@Before
	public void init() {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			con = DriverManager.getConnection("jdbc:hive2://darhhdlm1:10000/default", "", "");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void aaa() {
		
	}
	
	@Test
	public void createTableTest() {
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" create table employees ( ");
			sb.append(" name string , ");
			sb.append(" salary float , ");
			sb.append(" subordinates array<string> , ");
			sb.append(" deductions map<string,float> , ");
			sb.append(" address struct<street:string, city:string, state:string, zip:int> ");
			sb.append(" )  ");
			sb.append(" row format delimited ");
			//對照 ASCII
			//https://zh.wikipedia.org/wiki/ASCII
			//https://read01.com/6x6E4N.html
			sb.append(" fields terminated by '\001' ");//^A
			sb.append(" collection items terminated by '\002' ");//^B
			sb.append(" map keys terminated by '\003' ");//^C
			sb.append(" lines terminated by '\n' ");
			sb.append(" stored as textfile ");
			String sql = sb.toString();
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void createTableTempTest() {
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" create table employees_temp ( ");
			sb.append(" name string , ");
			sb.append(" salary string , ");
			sb.append(" subordinates string , ");
			sb.append(" deductions string ");
			sb.append(" )  ");
			sb.append(" row format delimited ");
			sb.append(" fields terminated by '\001' ");
//			sb.append(" collection items terminated by '^B' ");
//			sb.append(" map keys terminated by '^C' ");
			sb.append(" lines terminated by '\n' ");
			sb.append(" stored as textfile ");
			String sql = sb.toString();
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void createEncodeTableTest() {
		StringBuilder sb = new StringBuilder();
		sb.append("create external table if not exists AAAFN1 ("
				+ " id string,name_encode string,gender string,age string,addr string )"
				+ " row format SERDE 'com.cub.hdcos.hive.formater.EncodeSerDes' ");//location '/test'
		String sql = sb.toString();
		try {
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void queryAaafn1() {
		String sql = "SELECT * FROM cubinfotypeb";
		ResultSet res = null;
		try {
			stmt = con.prepareStatement(sql);
			res = stmt.executeQuery();
			while (res.next()) {
				String id = res.getString(1);
				String nameEnocde = res.getString(2);
				String tel = res.getString(3);
				String addr = res.getString(4);
				String team = res.getString(5);
				System.out.println(id + " , " + nameEnocde + " , " + tel + " , " + addr + " , " + team);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				res.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void loadDataTest() {
		String filePath = "/home/cloudera/employee.txt";
		String tableName = "employees";
		String sql = "load data local inpath '" + filePath + "' into table " + tableName;
		try {
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void loadDataTempTest() {
		String filePath = "/home/cloudera/employee_temp.txt";
		String tableName = "employees_temp";
		String sql = "load data local inpath '" + filePath + "' into table " + tableName;
		try {
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void loadAAAfn1Test() {
		String filePath = "/home/cloudera/test/AAAFN1.txt";
		String tableName = "AAAFN1";
		String sql = "load data local inpath '" + filePath + "' into table " + tableName;
		try {
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void insertSelectTable() {
		try {
			String sourceTable = "cub.apidis_samtesttable_20170208";
			String insertTable = "cub_restore.apidis_samtesttable_20170208";
			StringBuffer sql = new StringBuffer();
			sql.append("insert overwrite table ");
			sql.append(insertTable);
			sql.append(" select * from ");
			sql.append(sourceTable);
			System.out.println(sql.toString());
			executeUpdate(sql.toString());
			//insert overwrite table cub_restore.apidis_samtesttable_20170208 select * from cub.apidis_samtesttable_20170208
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void executeUpdate(String sql) throws SQLException {
		stmt = con.prepareStatement(sql);
		stmt.executeUpdate();
	}
	
	
	@After
	public void end() {
		try {
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
}
