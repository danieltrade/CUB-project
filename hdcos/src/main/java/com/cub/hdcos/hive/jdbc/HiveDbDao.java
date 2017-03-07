package com.cub.hdcos.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.cub.hdcos.hive.config.HiveConfig;

public abstract class HiveDbDao {
	
	final static Logger logger = Logger.getLogger(HiveDbDao.class);
	
	protected Connection con;
	
	protected PreparedStatement state;
	
	protected ResultSet rs;
	
	public HiveDbDao(String db) {
		try {
			Class.forName(HiveConfig.DRIVER);
			con = DriverManager.getConnection(HiveConfig.CONNECTION_URL + db);
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
		} catch (ClassNotFoundException e) {
			logger.error(e.getMessage() , e);
		}
	}
	
	public Connection getConnection() {
		return con;
	}
	
	public void closeConnection() {
		if(con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void createPreparedStatement(String sql) throws SQLException {
		state = con.prepareStatement(sql);
	}
	
	public void closePreparedStatement() {
		if(state != null) {
			try {
				state.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void closeResultSet() {
		if(rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int executeUpdate(String sql) throws SQLException {
		printSQL(sql);
		createPreparedStatement(sql);
		return state.executeUpdate();
	}
	
	public void executeQuery(String sql) throws SQLException {
		printSQL(sql);
		createPreparedStatement(sql);
		rs = state.executeQuery();
	}
	
	public void close() {
		closeResultSet();
		closePreparedStatement();
		closeConnection();
	}
	
	public void printSQL(String sql) {
		logger.info("execute hive SQL => " + sql);
	}
}
