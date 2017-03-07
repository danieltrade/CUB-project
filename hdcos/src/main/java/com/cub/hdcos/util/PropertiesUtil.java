package com.cub.hdcos.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
	
	public final static String HDCOS_CONFIG_PATH = "./hdcos-config.properties";
	
	public final static String CUB_ERRORCODE_PATH = "./cub-errorcode.properties";
	
	
	public static Properties getProperties(String propertiesPath) {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(propertiesPath);
//			input = PropertiesUtil.class.getResourceAsStream(propertiesPath);
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	
}
