package com.cub.hdcos.hdfs.util.test;

import org.apache.hadoop.fs.FileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hdfs.util.HdfsOperater;

public class HdfsOperaterTest {
	
	HdfsOperater operater;
	
	@Before
	public void init() {
		System.out.println("=========== init ===========");
		try {
			operater = HdfsOperater.getOperater();
		} catch (CUBException e) {
		}
	}
	@Test
	public void hdfsCreateDirectoryTest() {
		String dirPath = "/cathay/bank";
		try {
			boolean createSuccess = operater.createDirectory(dirPath);
			System.out.println("createSuccess : " + createSuccess);
		} catch (CUBException e) {
			e.printStackTrace();
		}
		
	}
	@Test
	public void hdfsDeleteDirectoryTest() {
		try {
			String delPath = "/cathay";
			boolean delSuccess = operater.deleteDirectory(delPath, true);
			System.out.println("delSuccess : " + delSuccess);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void hdfsPutFileTest() {
		try {
			String inPath = "/Users/daniel/myFile/CUB/AAAFN1.txt";
			//String outPath = "/cathay/bank/AAAFN1.txt";
			String outPath = "/test/AAAFN1.txt";
			operater.putFile(inPath, outPath);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void hdfsDelFileTest() {
		try {
			String delPath = "/cathay/bank/AAAFN1.txt";
			boolean delSuccess = operater.deleteFile(delPath);
			System.out.println("delSuccess : " + delSuccess);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void catFileTest() {
		try {
			String filePath = "hdfs://quickstart.cloudera:8020/cathay/bank/AAAFN1.txt";
			operater.catFile(filePath);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void getFileStatusTest() {
		try {
			String filePath = "/cathay/bank/AAAFN1.txt";
			FileStatus fs = operater.getFileStatus(filePath);
			String pathe = fs.getPath().toString();
			long accessTime = fs.getAccessTime();
			long length = fs.getLen();
			System.out.println("pathe : " + pathe + " , accessTime : " + accessTime + " , length : " + length);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void getFileStatusListTest() {
		try {
			String filePath = "/cathay/bank/";
			FileStatus[] fss = operater.getListStatus(filePath);
			for(FileStatus fs : fss) {
				String pathe = fs.getPath().toString();
				long accessTime = fs.getAccessTime();
				long length = fs.getLen();
				System.out.println("pathe : " + pathe + " , accessTime : " + accessTime + " , length : " + length);
			}
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void useGlobPatternTest() {
		try {
			//取得2007及2008年，所有2日的資料
			String filePath = "/dateDirTest/200[7-8]/*/02";
			FileStatus[] fss = operater.getGlobStatus(filePath);
			for(FileStatus fs : fss) {
				String pathe = fs.getPath().toString();
				System.out.println("pathe : " + pathe); 
			}
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@After
	public void endWork() {
		operater.clcose();
		System.out.println("=========== endWork ===========");
	}
}
