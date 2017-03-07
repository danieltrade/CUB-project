package com.cub.encrypt.util;

public class StringUtil {
	
	public static String parseNull(Object text) {
		return parseNull(String.valueOf(text));
	}
	
	public static String parseNull(String text) {
		if(text == null || "null".equals(text.toLowerCase())) {
			return "";
		}
		return text;
	}
	
	public static String parseNullToBlank(String text) {
		if(text == null || "".equals(text) || "null".equals(text.toLowerCase())) {
			return " ";
		}
		return text;
	}
	
	public static boolean isNotBlank(String text) {
		return !"".equals(parseNull(text));
	}
	
}
