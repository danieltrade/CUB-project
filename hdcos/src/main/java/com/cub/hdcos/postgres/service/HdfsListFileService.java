package com.cub.hdcos.postgres.service;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.postgres.jdbc.PostgresDbDao;

public class HdfsListFileService extends PostgresDbDao {
	
	final static Logger logger = Logger.getLogger(HdfsListFileService.class);
	
	public static HdfsListFileService getInstance(String db) {
		return new HdfsListFileService(db);
	}
	
	private HdfsListFileService(String db) {
		super(db);
	}
	
	public String querySourceFilePath(String sourceFileName) throws CUBException {
		String sourceFilePath = "";
		StringBuffer sql = new StringBuffer();
		sql.append(" SELECT A.file_path ||'/'|| A.file_name as sourceFilePath FROM            ");
		sql.append(" (SELECT file_name , file_path FROM Hdfs_LIST_FILE_INFO) A                ");
		sql.append(" where A.file_name = (SELECT SOURCE_FILE_NAME||'.'||file_type as fileName ");
		sql.append(" FROM SYS_SEND_DATA_LIST where SOURCE_FILE_NAME = ?                       ");
		sql.append(" GROUP BY SOURCE_FILE_NAME, file_type)                                    ");
		try {
			setParams(sourceFileName);
			executeQuery(sql.toString());
			while (rs.next()) {
				sourceFilePath = rs.getString("sourceFilePath");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_SYSSENDDATALIST_001 , MetaInfoService.class.getName());
		}
		return sourceFilePath;
	}
	
}
