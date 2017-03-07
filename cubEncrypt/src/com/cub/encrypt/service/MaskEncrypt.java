package com.cub.encrypt.service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class MaskEncrypt implements CubEncrypt {
	
	private String sensitivityCode;
	
	private String encodeMethod;
	
	private static String encodeChar = "X";
	
	public MaskEncrypt(String sensitivityCode , String encodeMethod) {
		this.sensitivityCode = sensitivityCode;
		this.encodeMethod = encodeMethod;
	}
	
	public String encodeValue(String text) throws Exception {
		String retStr = "";
		Class c = this.getClass();
		try {
			Class[] oParam = new Class[2];
			oParam[0] = String.class;
			oParam[1] = String.class;
			Constructor constructor = c.getConstructor(oParam);
			Object[] paramObjs = new Object[2];
			paramObjs[0] = sensitivityCode;
			paramObjs[1] = encodeMethod;
			Object obj = constructor.newInstance(paramObjs);
			Class[] mParam1 = {String.class};
			Method encryptStr = c.getMethod(encodeMethod , mParam1);
			Object[] mParamObjs1 = { text };
			Object ret = encryptStr.invoke(obj, mParamObjs1);
			retStr = (String) ret;
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw e;
		} catch (SecurityException e) {
			e.printStackTrace();
			throw e;
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw e;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw e;
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw e;
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			throw e;
		}
		return retStr;
	}
	
	public String b11_Encrypt(String text) {
		return maskText(text, 1);
	}
	
	public String b21_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, (text.length() - 2)));
		encodeStr.append(15);
		return encodeStr.toString();
	}
	
	public String b31_Encrypt(String text) {
		return maskText(text, 6);
	}
	
	public String b41_Encrypt(String text) {
		return encryptLikeTel(text, 5);
	}
	
	public String b42_Encrypt(String text) {
		return encryptLikeTel(text, 4);
	}
	
	public String b51_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		if(text.contains("@")) {
			String email = text.split("@")[0];
			String domain = text.split("@")[1];
			if(email.length() < 2) {
				encodeStr.append(email);
			} else {
				encodeStr.append(email.substring(0, 2));
				String tempVal = email.substring(2, email.length());
				for(int i = 0 ; i < tempVal.length() ; i++) {
					encodeStr.append(encodeChar);
				}
			}
			encodeStr.append("@");
			encodeStr.append(domain);
		}
		return encodeStr.toString();
	}
	
	private String encryptLikeTel(String text , int containCharLength) {
		StringBuffer encodeStr = new StringBuffer();
		if(text.contains("-")) {
			String btel = text.split("-")[0];
			String atel = text.split("-")[1];
			int btelLength = btel.length();
			if(btelLength < containCharLength) {
				int needLength = containCharLength - btelLength;
				encodeStr.append(text.substring(0, btelLength));
				encodeStr.append("-");
				encodeStr.append(atel.substring(0, needLength));
				String tempVal = atel.substring(needLength , atel.length());
				for(int i = 0 ; i < tempVal.length() ; i++) {
					encodeStr.append(encodeChar);
				}
				return encodeStr.toString();
			} else if(btelLength > containCharLength) {
				encodeStr.append(text.substring(0, containCharLength));
				int needLength = btelLength - containCharLength;
				for(int i = 0 ; i < needLength ; i++) {
					encodeStr.append(encodeChar);
				}
				encodeStr.append("-");
				for(int i = 0 ; i < atel.length() ; i++) {
					encodeStr.append(encodeChar);
				}
			} else {
				encodeStr.append(text.substring(0, containCharLength));
				encodeStr.append("-");
				for(int i = 0 ; i < atel.length() ; i++) {
					encodeStr.append(encodeChar);
				}
			}
			return encodeStr.toString();
		}
		encodeStr.append(text.substring(0, containCharLength));
		String tempVal = text.substring(containCharLength, text.length());
		for(int i = 0 ; i < tempVal.length() ; i++) {
			encodeStr.append(encodeChar);
		}
		return encodeStr.toString();
	}
	
	private String maskText(String text , int containLength) {
		int textLength = text.length();
//		if(textLength <= containLength) {
//			return text;
//		}
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, containLength));
		String tempVal = text.substring(containLength, textLength);
		for(int i = 0 ; i < tempVal.length() ; i++) {
			encodeStr.append(encodeChar);
		}
		return encodeStr.toString();
	}
	
	public static void main(String[] args) {
		try {
			String text = "t@gmail.com";
			String encryptText = CubEncryptUtil.getInstance("B51").encodeValue(text);
			System.out.println(encryptText);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
