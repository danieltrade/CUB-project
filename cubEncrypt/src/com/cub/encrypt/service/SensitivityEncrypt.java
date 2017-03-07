package com.cub.encrypt.service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.cub.encrypt.jnicall.service.EncryptService;

public class SensitivityEncrypt implements CubEncrypt {
	
	private String sensitivityCode;
	
	private String encodeMethod;
	
	private EncryptService service;
	
	public SensitivityEncrypt(String sensitivityCode , String encodeMethod) {
		this.service = new EncryptService();
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
	
	public String a01_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 2));
		encodeStr.append(service.encodeValue(text.substring(2, 10)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	
	public String a02_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 2));
		encodeStr.append(service.encodeValue(text.substring(2, 10)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	
	public String a03_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 2));
		encodeStr.append(service.encodeValue(text.substring(2, 10)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	
	public String a11_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 3));
		encodeStr.append(service.encodeValue(text.substring(3, 11)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	public String a12_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 7));
		encodeStr.append(service.encodeValue(text.substring(7, 15)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	public String a13_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 3));
		encodeStr.append(service.encodeValue(text.substring(4, 12)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	public String a14_Encrypt(String text) {
		StringBuffer encodeStr = new StringBuffer();
		encodeStr.append(text.substring(0, 3));
		encodeStr.append(service.encodeValue(text.substring(4, 12)));
		encodeStr.append(text.substring(text.length()));
		return encodeStr.toString();
	}
	
	public static void main(String[] args) {
		try {
			String text = "Daniel";
			String encryptText = CubEncryptUtil.getInstance("A01").encodeValue(text);
			System.out.println(encryptText);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
