package com.cub.hdcos.shell.execute;

import org.apache.log4j.Logger;

import com.cub.hdcos.command.MultiHdcosCommand;
import com.cub.hdcos.command.hqlcommand.DownladHdfsFileToLocal;
import com.cub.hdcos.command.hqlcommand.RenameHiveTempFile;
import com.cub.hdcos.common.type.RestoreStatus;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.util.CubFileUtil;

public class HiveTableRestoreShell extends BaseShell {
	
	final static Logger logger = Logger.getLogger(HiveTableRestoreShell.class);
	
	private String tableName;
	private String downloadPath;
	private String sourceFilePath;
	private String sourcePath;
//	private MetaDataList metaDatas;
	
	public static void main(String[] args) {
		String tableName = args[0];
		HiveTableRestoreShell shell = new HiveTableRestoreShell(tableName);
		shell.execute(logger);
	}
	
	public HiveTableRestoreShell(String tableName) {
		this.tableName = tableName;
	}
	
	public void beforeCommand() throws CUBException {
		updateHiveRestoreStatus(tableName, RestoreStatus.RUNNING);
//		this.metaDatas = getMetaDataList(tableName);
		
		String apId = tableName.split("_")[0];
		String sourceFileName = tableName.split("_")[1];
		this.sourceFilePath = getSourceFilePath(tableName);
		this.downloadPath = HiveConfig.RESTORE_HDFS_TEMP_PATH + tableName.toLowerCase() + "/" + sourceFileName;//hive warehouse底下目錄
		sourcePath = CubFileUtil.getFilePath(sourceFilePath);
		CubFileUtil.createDirIfNotExist(sourcePath);
		boolean deleteLocalFileSuccess = CubFileUtil.deleteExixtLocalFile(sourceFilePath);
		logger.info("delete localFile is " + deleteLocalFileSuccess);
	}
	
	public void executeCommand() throws CUBException {
		MultiHdcosCommand multiHdcosCommand = new MultiHdcosCommand();
		/*
		multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_RESTORE_DB));
		multiHdcosCommand.addCommand(new CreateRestoreTempTable(tempTableName , HiveConfig.RESTORE_HDFS_TEMP_PATH + tableName , metaDatas));
		multiHdcosCommand.addCommand(new InsertSelectTable(tempTableName, HiveConfig.CUB_DB + "." + tableName));
		multiHdcosCommand.addCommand(new DownladHdfsFileToLocal(true , downloadPath, localPath));
		multiHdcosCommand.addCommand(new DropHiveTable(tempTableName));
		multiHdcosCommand.addCommand(new RenameHiveTempFile(localPath , localFilePath));
		*/
		logger.info("downloadPath is " + downloadPath + " , localPath is " + sourcePath);
		multiHdcosCommand.addCommand(new DownladHdfsFileToLocal(false , downloadPath, sourcePath));
		multiHdcosCommand.addCommand(new RenameHiveTempFile(sourcePath , sourceFilePath));
		multiHdcosCommand.execute();
	}

	public void afterCommand() throws CUBException {}

	public void cubExceptionWork() throws CUBException {
		updateHiveRestoreStatus(tableName, RestoreStatus.FAIL);
	}

	public void exceptionWork() throws CUBException {
		updateHiveRestoreStatus(tableName, RestoreStatus.FAIL);
	}

	public void finallyWork() throws CUBException {
		logger.info("finallyWork localFilePath is " + sourceFilePath + " , " + CubFileUtil.checkFileExists(sourceFilePath));
		if(CubFileUtil.checkFileExists(sourceFilePath)) {
			updateHiveRestoreStatus(tableName, RestoreStatus.SUCCESS);
		}
	}
	
}
