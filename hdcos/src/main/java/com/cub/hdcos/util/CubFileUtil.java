package com.cub.hdcos.util;

import java.io.File;

public class CubFileUtil {
	
	public static boolean checkFileExists(String filePath) {
		File localFile = new File(filePath);
		return localFile.exists();
	}
	
	public static boolean createDirIfNotExist(String filePath) {
		if(!checkFileExists(filePath)) {
			File newFile = new File(filePath);
			return newFile.mkdirs();
		}
		return true;
	}
	
	public static boolean deleteDir(File file) {
		File[] contents = file.listFiles();
		if (contents != null) {
			for (File f : contents) {
				deleteDir(f);
			}
		}
		return file.delete();
	}
	
	public static boolean deleteExixtLocalFile(String localPath) {
		boolean deleteLocalFileSuccess = false;
		File localFile = new File(localPath);
		if(localFile.exists()) {
			deleteLocalFileSuccess = deleteDir(localFile);
		}
		return deleteLocalFileSuccess;
	}
	
	public static String getFilePath(String fileNamePath) {
		if("".equals(fileNamePath)) {
			return "";
		}
		return fileNamePath.substring(0, (fileNamePath.lastIndexOf("/")));
	}
	
	public static String getFileName(String fileNamePath) {
		if("".equals(fileNamePath)) {
			return "";
		}
		return fileNamePath.substring((fileNamePath.lastIndexOf("/") + 1), fileNamePath.lastIndexOf("."));
	}
	
	public static String getSubFileName(String fileNamePath) {
		if("".equals(fileNamePath)) {
			return "";
		}
		return fileNamePath.substring((fileNamePath.lastIndexOf(".") + 1));
	}
	
	public static String getFileFullName(String fileNamePath) {
		if("".equals(fileNamePath)) {
			return "";
		}
		return fileNamePath.substring((fileNamePath.lastIndexOf("/") + 1));
	}
}
