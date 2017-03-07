package com.cub.hdcos.postgres.service;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.postgres.jdbc.PostgresDbDao;

public class SysSendDataService extends PostgresDbDao {
	
	final static Logger logger = Logger.getLogger(SysSendDataService.class);
	
	public static SysSendDataService getInstance(String db) {
		return new SysSendDataService(db);
	}
	
	private SysSendDataService(String db) {
		super(db);
	}
	
	public String querySourceFilePath(String apId , String sourceFileName) throws CUBException {
		String sourceFilePath = "";
		StringBuffer sql = new StringBuffer();
		sql.append("select send_target_dir||'/'||source_file_name||'.'||file_type as sourceFilePath from sys_send_data_list where ap_id = ? and source_file_name = ? ");
		try {
			setParams(apId , sourceFileName);
			executeQuery(sql.toString());
			while (rs.next()) {
				sourceFilePath = rs.getString("sourceFilePath");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.debug(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_SYSSENDDATALIST_001 , SysSendDataService.class.getName());
		}
		return sourceFilePath;
	}
	public String querySourceFilePath(String tableName) throws CUBException {
		String sourceFilePath = "";
		StringBuffer sql = new StringBuffer();
		sql.append("select send_target_dir||'/'||source_file_name||'.'||file_type as sourceFilePath from sys_send_data_list where hivetablename = ? ");
		try {
			setParams(tableName);
			executeQuery(sql.toString());
			while (rs.next()) {
				sourceFilePath = rs.getString("sourceFilePath");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.debug(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_SYSSENDDATALIST_001 , SysSendDataService.class.getName());
		}
		return sourceFilePath;
	}
	
}
