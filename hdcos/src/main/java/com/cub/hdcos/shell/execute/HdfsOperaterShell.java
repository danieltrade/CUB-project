package com.cub.hdcos.shell.execute;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hdfs.util.HdfsOperater;
import com.cub.hdcos.util.CubFileUtil;

public class HdfsOperaterShell extends BaseShell {
	
	final static Logger logger = Logger.getLogger(HdfsOperaterShell.class);
	
	public static final String ROOT_PATH = "/CUB/";
	
	private String apId;
	private String filePath;
	private String orgFileName;
	
	private String inPath;
	private String dirPath;
	private String outPath;
	
	public HdfsOperaterShell(String apId , String filePath , String orgFileName) {
		this.apId = apId;
		this.filePath = filePath;
		this.orgFileName = orgFileName;
	}
	
	public static void main(String[] args) {
		String apId = args[0];//COMR6BKC01
		String filePath = args[1];///COMR6BKC01/TEST/20170101/TEST
		String orgFileName = args[2];//TEST.txt
		HdfsOperaterShell shell = new HdfsOperaterShell(apId , filePath , orgFileName);
		shell.execute(logger);
	}
	
	public void beforeCommand() throws CUBException {
		this.inPath = filePath + "/" + orgFileName;
		this.dirPath = getHDFSDirPath(apId, orgFileName);
		this.outPath = dirPath + "/" + orgFileName;
	}

	public void executeCommand() throws CUBException {
		HdfsOperater operater = HdfsOperater.getOperater();
		boolean mkdirSuccess = operater.createDirectory(dirPath);
		operater.putFile(inPath, outPath);
		operater.clcose();
	}

	public void afterCommand() throws CUBException {}

	public void cubExceptionWork() throws CUBException {}

	public void exceptionWork() throws CUBException {}

	public void finallyWork() throws CUBException {
		HdfsOperater operater = HdfsOperater.getOperater();
		if(operater.checkFileExists(outPath)) {
			logger.info("run HdfsOperaterShell outPath : " + outPath + " success!");
		} else {
			logger.info("run HdfsOperaterShell outPath : " + outPath + " failed!");
		}
		operater.clcose();
	}
	
	private static String getHDFSDirPath(String apId , String orgFileName) {
		String fileName = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDate = sdf.format(new Date());
		if(orgFileName.contains(".")) {
			String[] fileNameAry = orgFileName.split("[.]");
			fileName = fileNameAry[0]; 
		}
		if(orgFileName.contains("_")) {
			String[] fileNameAry = orgFileName.split("_");
			fileName = fileNameAry[0];
			String date = fileNameAry[1].split("[.]")[0];
			int dateLength = date.length();
			if(dateLength == 6) {
				date += "01";
			} else if(dateLength == 4) {
				date += "0101";
			}
			fileDate = date;
		}
		String dirPath = ROOT_PATH + apId + "/" + fileName + "/" + fileDate ;
		return dirPath;
	}
	
}
