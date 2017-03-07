package com.cub.hdcos.hive.config;

import java.util.Properties;

import com.cub.hdcos.util.PropertiesUtil;

public class HiveConfig {
	
	public static String DRIVER;
	
	public static String CONNECTION_URL;
	
	public static String DEFAULT_DB;
	
	public static String CUB_DB;
	
	public static String CUB_ENCRYPTE_DB;
	
	public static String CUB_RESTORE_DB;
	
	public static String RESTORE_HDFS_TEMP_PATH;
	
	static {
		Properties prop = PropertiesUtil.getProperties(PropertiesUtil.HDCOS_CONFIG_PATH);
		DRIVER = prop.getProperty("hive.jdbc.driver");
		CONNECTION_URL = prop.getProperty("hive.connection.url");
		DEFAULT_DB = prop.getProperty("hive.default.db");
		CUB_DB = prop.getProperty("hive.db.cub");
		CUB_ENCRYPTE_DB = prop.getProperty("hive.db.cub_encrypt");
		CUB_RESTORE_DB = prop.getProperty("hive.db.cub_restore");
		RESTORE_HDFS_TEMP_PATH = prop.getProperty("hive.restore.hdfs.temp.path");
		
		System.out.println("####### DRIVER : " + DRIVER);
		System.out.println("####### CONNECTION_URL : " + CONNECTION_URL);
		System.out.println("####### DEFAULT_DB : " + DEFAULT_DB);
		System.out.println("####### CUB_DB : " + CUB_DB);
		System.out.println("####### CUB_ENCRYPTE_DB : " + CUB_ENCRYPTE_DB);
		
//		DRIVER = "org.apache.hive.jdbc.HiveDriver";
//		CONNECTION_URL = "jdbc:hive2://quickstart.cloudera:10000/";
//		DEFAULT_DB = "default";
//		CUB_DB = "cub";
//		CUB_ENCRYPTE_DB = "cub_encrypt";
	}
}
