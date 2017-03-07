package com.cub.hdcos.hdfs.util;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.util.PropertiesUtil;

public class HdfsOperater {
	
	final static Logger logger = Logger.getLogger(HdfsOperater.class);
	
	private static FileSystem hdfs;
	
	public static HdfsOperater getOperater() throws CUBException {
		try {
			Properties prop = PropertiesUtil.getProperties(PropertiesUtil.HDCOS_CONFIG_PATH);
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", prop.getProperty("hdfs.fs.defaultFS"));
			conf.set("dfs.nameservices", prop.getProperty("hdfs.dfs.nameservices"));
			conf.set("dfs.ha.namenodes.nameservice1", prop.getProperty("hdfs.dfs.ha.namenodes.nameservice1"));
			conf.set("dfs.namenode.rpc-address.nameservice1.namenode76", prop.getProperty("hdfs.dfs.namenode.rpc-address.nameservice1.namenode76"));
			conf.set("dfs.namenode.rpc-address.nameservice1.namenode81", prop.getProperty("hdfs.dfs.namenode.rpc-address.nameservice1.namenode81"));
			conf.set("dfs.client.failover.proxy.provider.nameservice1", prop.getProperty("hdfs.dfs.client.failover.proxy.provider.nameservice1"));
			conf.set("fs.hdfs.impl", prop.getProperty("hdfs.fs.hdfs.impl")); 
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_001 , HdfsOperater.class.getName());
		}
		return new HdfsOperater();
	}
	
	private HdfsOperater() {}
	
	/**
	 * 建立目錄
	 * @param dirPath String (建立HDFS目錄路徑)
	 * @throws CUBException 
	 * */
	public boolean createDirectory(String dirPath) throws CUBException {
		boolean mkdirSuccess = false;
		try {
			Path mkdirPath = new Path(dirPath); // "/cathay/bank"
			mkdirSuccess = hdfs.mkdirs(mkdirPath);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_002 , HdfsOperater.class.getName());
		}
		return mkdirSuccess;
	}
	/**
	 * 刪除目錄
	 * @param delPath String (刪除HDFS目錄路徑)
	 * @param recursive boolean (是否連同目錄內容刪除)
	 * */
	public boolean deleteDirectory(String delPath , boolean recursive) throws CUBException {
		boolean delSuccess = false;
		try {
			Path delDirPath = new Path(delPath);
			delSuccess = hdfs.delete(delDirPath, recursive);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_003 , HdfsOperater.class.getName());
		}
		return delSuccess;
	}
	/**
	 * 刪除檔案
	 * @param delPath String (刪除HDFS檔案路徑)
	 * @throws CUBException 
	 * */
	public boolean deleteFile(String delPath) throws CUBException {
		return deleteDirectory(delPath , false);
	}
	/**
	 * 放置檔案至HDFS
	 * @param inPath String (local檔案路徑)
	 * @param outPath String (HDFS檔案路徑)
	 * @throws CUBException 
	 * */
	public void putFile(String inPath , String outPath) throws CUBException {
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(inPath));
			Path putPath = new Path(outPath);
			OutputStream fsout = hdfs.create(putPath, true);
			IOUtils.copyBytes(in, fsout, 4096, true);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_004 , HdfsOperater.class.getName());
		}
	}
	
	/**
	 * 下載HDFS檔案至local
	 * @param deleteSorce boolean (下載後是否刪除HDFS上檔案)
	 * @param downloadPath String (下載HDFS檔案路徑)
	 * @param localPath String (local放置檔案路徑)
	 * @throws CUBException 
	 * */
	public void downladHdfsFileToLocal(boolean deleteSorce , String downloadPath , String localPath) throws CUBException {
		try {
			Path download = new Path(downloadPath);
			Path localDir = new Path(localPath);
			hdfs.copyToLocalFile(deleteSorce , download, localDir);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_005 , HdfsOperater.class.getName());
		}
	}
	/**
	 * 看HDFS上的檔案內容
	 * @param hdfsFilePath String (HDFS檔案路徑)
	 * @throws CUBException 
	 * */
	public void catFile(String hdfsFilePath) throws CUBException {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		InputStream in = null;
		try {
			in = new URL(hdfsFilePath).openStream();
			IOUtils.copyBytes(in, System.out, 4096 , false);
		} catch (MalformedURLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_006 , HdfsOperater.class.getName());
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_006 , HdfsOperater.class.getName());
		} finally {
			IOUtils.closeStream(in);
		}
	}
	/**
	 * 取得HDFS上檔案狀態資訊
	 * @param filePath String (HDFS上檔案路徑)
	 * @throws CUBException 
	 * */
	public FileStatus getFileStatus(String filePath) throws CUBException {
		FileStatus fs = null;
		try {
			Path fPath = new Path(filePath);
			fs = hdfs.getFileStatus(fPath);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_007 , HdfsOperater.class.getName());
		}
		return fs;
	}
	/**
	 * 取得HDFS目錄下所有的狀態資訊
	 * @param filePath String (HDFS上檔案路徑)
	 * @throws CUBException 
	 * */
	public FileStatus[] getListStatus(String filePath) throws CUBException {
		FileStatus[] fss = null;
		try {
			Path fPath = new Path(filePath);
			fss = hdfs.listStatus(fPath);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_007 , HdfsOperater.class.getName());
		}
		return fss;
	}
	/**
	 * 取得HDFS目錄下所有的狀態資訊
	 * @param filePathPattern String (Ex:"hdfs://localhost:9000/user/*")
	 * @throws CUBException 
	 * */
	public FileStatus[] getGlobStatus(String filePathPattern) throws CUBException {
		FileStatus[] fss = null;
		try {
			Path pathPattern = new Path(filePathPattern);
			fss = hdfs.globStatus(pathPattern);
		} catch (IOException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_007 , HdfsOperater.class.getName());
		}
		return fss;
	}
	
	/***
	 * 移動檔案
	 */
	public boolean moveFile(String srcPath, String destPath) {
		boolean moveSuccess = false;
		try {
			moveSuccess = hdfs.rename(new Path(srcPath), new Path(destPath));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return moveSuccess;
	}
	/**
	 * 檢查HDFS檔案是否存在
	 * @param filePath String 
	 * @throws CUBException 
	 * */
	public boolean checkFileExists(String filePath) throws CUBException {
		boolean isExists = false;
		try {
			isExists = hdfs.exists(new Path(filePath));
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HDFS_008 , HdfsOperater.class.getName());
		}
		return isExists;
	}
	/**
	 * 關閉HDFS操作
	 * */
	public void clcose() {
		if(hdfs != null) {
			try {
				hdfs.close();
			} catch (IOException e) {
				logger.error(e.getMessage() , e);
			}
		}
	}
	
}
