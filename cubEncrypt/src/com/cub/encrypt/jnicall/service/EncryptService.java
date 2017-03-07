package com.cub.encrypt.jnicall.service;

import com.cub.encrypt.jnicall.entity.EcType;
//com.cub.encrypt.jnicall.service.EncryptService
public class EncryptService {
	
	public static String CUB_LIB_PATH = "/opt/cloudera/parcels/CDH/lib/hive/native/cubEncryptDES.so";
	
	{
		//String classPath = EncryptService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		//classPath = classPath.substring(0, (classPath.indexOf("bin"))) + "dll/cubEncryptDES.so";
		System.load(CUB_LIB_PATH);
//		System.loadLibrary("cubEncryptDES");
	}
	
	public NativeEncryptService service;
	
	public EncryptService() {
		service = new NativeEncryptService();
	}
	
	public String encodeValue(String text) {
		return encodeValue(text, "12345678" , EcType.SING_TXT_BY_SING_KEY);//12345678 for test
	}
	
	public String encodeValue(String text , String key) {
		return encodeValue(text, key , EcType.SING_TXT_BY_SING_KEY);
	}
	
	public String encodeValue(String text , String key , String ecType) {
		return service.nativeEncodeValue(text, key, ecType);
	}
	
	public static void main(String[] args) {
		EncryptService service = new EncryptService();
		if(args != null) {
			for(int i = 0 ; i < args.length ; i++ ) {
				String encodeVal = args[i];
				System.out.println("encodeValue(\"" + encodeVal + "\") : " + service.encodeValue(encodeVal));
				System.out.println("encodeValue(\"" + encodeVal + "\" , \"12345678\") : " + service.encodeValue(encodeVal , "12345678"));
				System.out.println("encodeValue(\"" + encodeVal + "\" , \"12345678\" , \"11\") : " + service.encodeValue(encodeVal , "12345678" , "11"));
			}
		} else {
			System.out.println("no parameters...");
		}
	}
	
}
