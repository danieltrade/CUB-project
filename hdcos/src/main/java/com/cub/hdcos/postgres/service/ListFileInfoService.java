package com.cub.hdcos.postgres.service;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.postgres.jdbc.PostgresDbDao;

public class ListFileInfoService extends PostgresDbDao {
	
	final static Logger logger = Logger.getLogger(ListFileInfoService.class);
	
	public static ListFileInfoService getInstance(String db) {
		return new ListFileInfoService(db);
	}
	
	private ListFileInfoService(String db) {
		super(db);
	}
	
	public boolean updateHiveRestoreStatus(String tableName , String restoreStatus) throws CUBException {
		int updateCnt = 0;
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("update hive_list_file_info set restore_status = ? where table_name = ? ");
			setParams(restoreStatus , tableName);
			updateCnt = executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_HIVELISTFILEINFO_001 , ListFileInfoService.class.getName());
		}
		return (updateCnt > 0);
	}
	
	
	public boolean updateHDFSRestoreStatus(String fileName, String filePath, String restoreStatus) throws CUBException {
		int updateCnt = 0;
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("update hdfs_list_file_info set restore_status = ? where file_name = ? and file_path = ? ");
			setParams(restoreStatus , fileName , filePath);
			updateCnt = executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_HDFSLISTFILEINFO_001 , ListFileInfoService.class.getName());
		}
		return (updateCnt > 0);
	}
	
}
