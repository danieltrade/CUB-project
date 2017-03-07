package com.cub.hdcos.shell.execute;

import org.apache.log4j.Logger;

import com.cub.hdcos.command.MultiHdcosCommand;
import com.cub.hdcos.command.hqlcommand.DownladHdfsFileToLocal;
import com.cub.hdcos.common.type.RestoreStatus;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.util.CubFileUtil;

public class HdfsRestoreShell extends BaseShell {
	
	final static Logger logger = Logger.getLogger(HdfsRestoreShell.class);
	
	private String downloadPath;
	private String downloadFileName;
	private String localPath;
	
	public HdfsRestoreShell(String downloadPath , String downloadFileName) {
		this.downloadPath = downloadPath;
		this.downloadFileName = downloadFileName;
	}
	
	public static void main(String[] args) {
		String downloadPath = args[0];// /CUB/COMR6BKC01/TEST/20170101
		String downloadFileName = args[1];//TEST.txt
		logger.info("downloadPath : " + downloadPath + " , downloadFileName : " + downloadFileName);
		HdfsRestoreShell shell = new HdfsRestoreShell(downloadPath , downloadFileName);
		shell.execute(logger);
	}
	
	public void beforeCommand() throws CUBException {
		updateHDFSRestoreStatus(downloadFileName , downloadPath , RestoreStatus.RUNNING);
		String apId = downloadPath.split("/")[2];
		String fileName = "";
		if(downloadFileName.contains(".")) {
			String[] fileNameAry = downloadFileName.split("[.]");
			fileName = fileNameAry[0];//TEST
		}
		String sourceFilePath = getSourceFilePath(apId, fileName);
		logger.info("sourceFilePath : " + sourceFilePath);
		localPath = CubFileUtil.getFilePath(sourceFilePath);
		CubFileUtil.createDirIfNotExist(localPath);
		boolean deleteLocalFileSuccess = CubFileUtil.deleteExixtLocalFile(sourceFilePath);
		logger.info("delete localFile is " + deleteLocalFileSuccess);
	}
	
	public void executeCommand() throws CUBException {
		MultiHdcosCommand multiHdcosCommand = new MultiHdcosCommand();
		String downloadPath = this.downloadPath + "/" + downloadFileName;///COMR6BKC01/TEST/20170101/TEST.txt
		logger.info("downloadPath is " + downloadPath + " , localPath is " + localPath);
		multiHdcosCommand.addCommand(new DownladHdfsFileToLocal(false , downloadPath, localPath));
		multiHdcosCommand.execute();
	}

	public void afterCommand() throws CUBException {}

	public void cubExceptionWork() throws CUBException {
		updateHDFSRestoreStatus(downloadFileName , downloadPath , RestoreStatus.FAIL);
	}

	public void exceptionWork() throws CUBException {
		updateHDFSRestoreStatus(downloadFileName , downloadPath , RestoreStatus.FAIL);
	}

	public void finallyWork() throws CUBException {
		if(CubFileUtil.checkFileExists(localPath + "/" + downloadFileName)) {
			updateHDFSRestoreStatus(downloadFileName , downloadPath , RestoreStatus.SUCCESS);///COMR6BKC01/TEST/20170101
		}
	}
}
