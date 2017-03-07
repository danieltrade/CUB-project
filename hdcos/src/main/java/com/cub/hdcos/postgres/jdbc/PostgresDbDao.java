package com.cub.hdcos.postgres.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cub.hdcos.postgres.config.PosgresConfig;


public class PostgresDbDao {
	
	final static Logger logger = Logger.getLogger(PostgresDbDao.class);
	
	protected Connection con;
	
	protected PreparedStatement state;
	
	protected ResultSet rs;
	
	protected List<String> params;
	
	public PostgresDbDao(String db) {
		try {
			Class.forName(PosgresConfig.DRIVER);
			con = DriverManager.getConnection(PosgresConfig.CONNECTION_URL + db , PosgresConfig.USER_NAME , PosgresConfig.USER_PSWD );
			params = new ArrayList<String>();
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
		try{
			createPreparedStatement(sql);
			setStateParams();
			printSQL(sql);
			printParams();
			return state.executeUpdate();
		} finally {
			clearParams();
		}
	}
	
	public void executeQuery(String sql) throws SQLException {
		createPreparedStatement(sql);
		setStateParams();
		printSQL(sql);
		printParams();
		rs = state.executeQuery();
		clearParams();
	}
	
	public void setStateParams() throws SQLException {
		if(params != null) {
			for(int i = 0 ; i < params.size(); i++) {
				String param = params.get(i);
				state.setString((i + 1), param);
			}
		}
	}
	
	public void setParams(String... params) {
		if(params != null) {
			for(String param : params) {
				this.params.add(param);
			}
		}
	}
	
	public void clearParams() {
		this.params.clear();
	}
	
	public void close() {
		closeResultSet();
		closePreparedStatement();
		closeConnection();
	}
	
	public void printSQL(String sql) {
		logger.info("execute postgres SQL => " + sql);
	}
	
	public void printParams() {
		if(params != null) {
			StringBuffer sb = new StringBuffer();
			sb.append("SQL params => ");
			for(int i = 0 ; i < params.size() ; i++) {
				if(i > 0) {
					sb.append(",");
				}
				String param = params.get(i);
				sb.append(param);
			}
			logger.info(sb.toString());
		}
	}
}
