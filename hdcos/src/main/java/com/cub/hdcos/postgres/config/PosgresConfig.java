package com.cub.hdcos.postgres.config;

import java.util.Properties;

import com.cub.hdcos.util.PropertiesUtil;

public class PosgresConfig {
	
	public static String DRIVER;
	
	public static String CONNECTION_URL;
	
	public static String DEFAULT_DB;
	
	public static String USER_NAME;
	
	public static String USER_PSWD;
	
	static {
		Properties prop = PropertiesUtil.getProperties(PropertiesUtil.HDCOS_CONFIG_PATH);
		DRIVER = prop.getProperty("postgres.jdbc.driver");
		CONNECTION_URL = prop.getProperty("postgres.connection.url");
		DEFAULT_DB = prop.getProperty("postgres.default.db");
		USER_NAME = prop.getProperty("postgres.username");
		USER_PSWD = prop.getProperty("postgres.pswd");
//		DRIVER = "";
//		CONNECTION_URL = "";
//		DEFAULT_DB = "cubtestdb";
	}
	
}
