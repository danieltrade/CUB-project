package com.cub.hdcos.shell.execute;

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.exception.Postzmsg;
import com.cub.hdcos.postgres.config.PosgresConfig;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.postgres.entity.MetaDataList;
import com.cub.hdcos.postgres.service.ListFileInfoService;
import com.cub.hdcos.postgres.service.MetaInfoService;
import com.cub.hdcos.postgres.service.SysSendDataService;
import com.cub.hdcos.util.PostzmsgLogger;
import com.cub.hdcos.util.PropertiesUtil;

public abstract class BaseShell {
	
	private final static PostzmsgLogger postzLogger = PostzmsgLogger.getLogger();
	
	private static Logger logger;
	
	public void execute(Logger logger) {
		try {
			this.logger = logger;
			beforeCommand();
			executeCommand();
			afterCommand();
		} catch (CUBException e) {
			logger.error(e.getMessage() , e);
			postzLogger.error(getPostzmsg(e));
			try {
				cubExceptionWork();
			} catch (CUBException e1) {
				logger.error(e1.getMessage() , e1);
				postzLogger.error(getPostzmsg(e));
			} catch(Exception e2) {
				logger.error(e2.getMessage() , e2);
				postzLogger.error(getPostzmsg(e2));
			}
		} catch(Exception e) {
			logger.error(e.getMessage() , e);
			postzLogger.error(getPostzmsg(e));
			try {
				exceptionWork();
			} catch (CUBException e1) {
				logger.error(e1.getMessage() , e1);
				postzLogger.error(getPostzmsg(e1));
			} catch(Exception e2) {
				logger.error(e2.getMessage() , e2);
				postzLogger.error(getPostzmsg(e2));
			}
		} finally {
			try {
				finallyWork();
			} catch (CUBException e) {
				logger.error(e.getMessage() , e);
				postzLogger.error(getPostzmsg(e));
			} catch(Exception e1) {
				logger.error(e1.getMessage() , e1);
				postzLogger.error(getPostzmsg(e1));
			}
		}
	}
	
	protected String getSourceFilePath(String tableName) throws CUBException {
		SysSendDataService sysService = SysSendDataService.getInstance(PosgresConfig.DEFAULT_DB);
		String sourceFilePath = sysService.querySourceFilePath(tableName);
		return sourceFilePath;
	}
	
	protected String getSourceFilePath(String apId, String sourceFileName) throws CUBException {
		SysSendDataService sysService = SysSendDataService.getInstance(PosgresConfig.DEFAULT_DB);
		String sourceFilePath = sysService.querySourceFilePath(apId, sourceFileName);
		return sourceFilePath;
	}
	
	protected MetaDataList getMetaDataList(String tableName) throws CUBException {
		MetaInfoService serviec = MetaInfoService.getInstance(PosgresConfig.DEFAULT_DB);
		MetaDataList metaDatas = serviec.queryMetaDataList(tableName);
		return metaDatas;
	}
	
	protected void updateHiveRestoreStatus(String tableName , String restoreStatus) throws CUBException {
		ListFileInfoService hivelistFileInfoService = ListFileInfoService.getInstance(PosgresConfig.DEFAULT_DB);
		boolean updStatus = hivelistFileInfoService.updateHiveRestoreStatus(tableName, restoreStatus);
		logger.info("update Hive RestoreStatus " + restoreStatus + " : " + updStatus);
	}
	
	protected void updateHDFSRestoreStatus(String fileName , String filePath , String restoreStatus) throws CUBException {
		ListFileInfoService hdfslistFileInfoService = ListFileInfoService.getInstance(PosgresConfig.DEFAULT_DB);
		boolean updStatus = hdfslistFileInfoService.updateHDFSRestoreStatus(fileName, filePath, restoreStatus);
		logger.info("update HDFS RestoreStatus " + restoreStatus + " : " + updStatus);
	}
	
	protected static void printInfo(MetaDataList metaDatas) {
		logger.info("FileName : " + metaDatas.getFileName());
		logger.info("FileFormat : " + metaDatas.getFileFormat());
		logger.info("Delmiter : " + metaDatas.getDelmiter());
		logger.info("FixformatMaxlength : " + metaDatas.getFixformatMaxlength());
		List<ColumninfoList> columnInfos = metaDatas.getColumns();
		for(ColumninfoList columnInfo : columnInfos) {
			logger.info("ColumnName : " + columnInfo.getColumnName());
		}
	}
	
	protected Postzmsg getPostzmsg(CUBException e) {
		Properties prop = PropertiesUtil.getProperties(PropertiesUtil.CUB_ERRORCODE_PATH);
		String errCode = e.getErrorCode();
		logger.info("CUB errCode : " + errCode);
		String errMsg = prop.getProperty(errCode);
		logger.info("CUB errMsg : " + errMsg);
		if(errMsg == null || "".equals(errMsg)) {
			return getNotCubExceptionMsg(e);
		}
		
		String severity = errMsg.split("[|]")[0];
		errMsg = errMsg.split("[|]")[1];
		
		Postzmsg postzMsg = new Postzmsg();
		postzMsg.setRtype(severity);
		postzMsg.setMtype(errMsg);
		postzMsg.setHostName(PostzmsgLogger.getHostName());
		postzMsg.setDate(PostzmsgLogger.getSysDate());
		postzMsg.setSubSource("CUB");
		postzMsg.setOrigin(e.getErrorClassName());
		postzMsg.setSubOrigin(errCode);
		postzMsg.setAdapterHost("錯誤碼[" + errCode + "]" + errMsg + "!");
		return postzMsg;
	}
	
	/**
	 * not CUB Exception
	 * 
	 * */
	protected Postzmsg getNotCubExceptionMsg(CUBException e) {
		Properties prop = PropertiesUtil.getProperties(PropertiesUtil.CUB_ERRORCODE_PATH);
		String errMsg = prop.getProperty(CUBErrorCode.CUB_COMMON_001);
		String errCode = CUBErrorCode.CUB_COMMON_001;
		
		String severity = errMsg.split("[|]")[0];
		errMsg = errMsg.split("[|]")[1];
		Postzmsg postzMsg = new Postzmsg();
		postzMsg.setRtype(severity);
		postzMsg.setMtype(e.getClass().getName());
		postzMsg.setHostName(PostzmsgLogger.getHostName());
		postzMsg.setSubSource("CUB");
		
		StringBuffer classNames = new StringBuffer();
		for (StackTraceElement ste : e.getStackTrace()) {
			String className = ste.getClassName();
			if(className.contains("hdcos")) {
				classNames.append(className);
//				classNames.append(" ");
				break;//抓excption最後發生的class
			}
		}
		postzMsg.setOrigin(classNames.toString());
		postzMsg.setSubOrigin(errCode);
		postzMsg.setAdapterHost("錯誤碼[" + errCode + "]" + errMsg + "!");
		return postzMsg;
	}
	
	protected Postzmsg getPostzmsg(Exception e) {
		Properties prop = PropertiesUtil.getProperties(PropertiesUtil.CUB_ERRORCODE_PATH);
		String errMsg = prop.getProperty(CUBErrorCode.CUB_COMMON_001);
		String errCode = CUBErrorCode.CUB_COMMON_001;
		
		String severity = errMsg.split("[|]")[0];
		errMsg = errMsg.split("[|]")[1];
		Postzmsg postzMsg = new Postzmsg();
		postzMsg.setRtype(severity);
		postzMsg.setMtype(e.getClass().getName());
		postzMsg.setHostName(PostzmsgLogger.getHostName());
		postzMsg.setSubSource("CUB");
		
		StringBuffer classNames = new StringBuffer();
		for (StackTraceElement ste : e.getStackTrace()) {
			String className = ste.getClassName();
			if(className.contains("hdcos")) {
				classNames.append(className);
//				classNames.append(" ");
				break;//抓excption最後發生的class
			}
		}
		postzMsg.setOrigin(classNames.toString());
		postzMsg.setSubOrigin(errCode);
		postzMsg.setAdapterHost("錯誤碼[" + errCode + "]" + errMsg + "!");
		return postzMsg;
	}
	
	abstract public void beforeCommand() throws CUBException;
	abstract public void executeCommand() throws CUBException;
	abstract public void afterCommand() throws CUBException;
	abstract public void cubExceptionWork() throws CUBException;
	abstract public void exceptionWork() throws CUBException;
	abstract public void finallyWork() throws CUBException;
}
