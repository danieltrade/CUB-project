package com.cub.hdcos.shell.execute;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.cub.hdcos.command.MultiHdcosCommand;
import com.cub.hdcos.command.hqlcommand.CreateDatabase;
import com.cub.hdcos.command.hqlcommand.CreateEncryptView;
import com.cub.hdcos.command.hqlcommand.CreateFixFormatTable;
import com.cub.hdcos.command.hqlcommand.CreateView;
import com.cub.hdcos.command.hqlcommand.DropHiveTable;
import com.cub.hdcos.command.hqlcommand.DropHiveView;
import com.cub.hdcos.command.hqlcommand.LoadHDFSFileIntoTable;
import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hdfs.util.HdfsOperater;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.postgres.entity.MetaDataList;
//com.cub.hdcos.shell.execute.HiveMetaShell
public class HiveMetaShell extends BaseShell {
	
final static Logger logger = Logger.getLogger(HiveMetaShell.class);
	
	private String tableName;
	private String outPath;
	private String fileName;
	private MetaDataList metaDatas;
	private HdfsOperater operater;
	
	public static void main(String[] args) {
		String tableName = args[0];
		HiveMetaShell shell = new HiveMetaShell(tableName);
		shell.execute(logger);
	}
	
	public HiveMetaShell(String tableName) {
		this.tableName = tableName;
	}
	
	public void beforeCommand() throws CUBException {
		this.metaDatas = getMetaDataList(tableName);
		String sourceFilePath = getSourceFilePath(tableName);
		this.outPath = "/tmp" + sourceFilePath.substring(0, sourceFilePath.lastIndexOf("/"));
		this.operater = HdfsOperater.getOperater();
		boolean mkdirSuccess = operater.createDirectory(outPath);
		this.fileName = metaDatas.getFileName();
		
		if(this.metaDatas.getFileCode().equals("ebcdic")) { // 如果Meta頁面設定編碼為EBCDIC，要先call jar解析來源檔案，轉碼為big5
			try {
				String exeCommand = "java -jar /tmp/hexConverter.jar " 
						+ sourceFilePath + " " + ("/tmp/tmpHex.txt") + " " + ("/tmp/" + fileName + "_ebcdic.txt");
				Process process = Runtime.getRuntime().exec(exeCommand);
				logger.info("hexConverter execute command : " + exeCommand);
				process.waitFor();
			} catch (IOException e) {
				logger.error(e.getMessage() , e);
				throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_012 , HiveMetaShell.class.getName());
			} catch (InterruptedException e) {
				logger.error(e.getMessage() , e);
				throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_012 , HiveMetaShell.class.getName());
			}
			
			this.metaDatas.setFileCode("big5");
			sourceFilePath = ("/tmp/" + fileName + "_ebcdic.txt");
		}
	 
		operater.putFile(sourceFilePath, outPath + "/" + fileName);
	}

	public void executeCommand() throws CUBException {
		MultiHdcosCommand multiHdcosCommand = new MultiHdcosCommand();
		multiHdcosCommand.addCommand(new DropHiveTable(tableName));
		multiHdcosCommand.addCommand(new CreateFixFormatTable(tableName , metaDatas));
		multiHdcosCommand.addCommand(new LoadHDFSFileIntoTable(outPath + "/" + fileName , tableName));
		multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_DB));
		multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_ENCRYPTE_DB));
		multiHdcosCommand.addCommand(new DropHiveView(HiveConfig.CUB_DB + "." + tableName));
		multiHdcosCommand.addCommand(new CreateView(metaDatas));
		multiHdcosCommand.addCommand(new DropHiveView(HiveConfig.CUB_ENCRYPTE_DB + "." + tableName));
		multiHdcosCommand.addCommand(new CreateEncryptView(metaDatas));
		multiHdcosCommand.execute();
	}

	public void afterCommand() throws CUBException {
		this.operater.deleteDirectory(outPath, true);
		
		// 刪除解析EBCDIC編碼產生的暫存檔案與解析後檔案
		delFile("/tmp/tmpHex.txt");  
		delFile("/tmp/" + fileName + "_ebcdic.txt");
		// 刪除解析EBCDIC編碼產生的暫存檔案與解析後檔案
		
		this.operater.clcose();
	}

	public void cubExceptionWork() throws CUBException {}

	public void exceptionWork() throws CUBException {}

	public void finallyWork() throws CUBException {}
	
	public void delFile(String filePathAndName) {
		try {
			File myDelFile = new File(filePathAndName);
			myDelFile.delete();
		} catch(Exception e) {
			logger.error(e.getMessage() , e);
		}
	}
}
