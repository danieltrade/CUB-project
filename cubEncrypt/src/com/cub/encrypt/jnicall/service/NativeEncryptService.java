package com.cub.encrypt.jnicall.service;
//com.cub.encrypt.jnicall.service.NativeEncryptService
public class NativeEncryptService {
	
	public native String nativeEncodeValue(String text , String key , String ecType);
	
}
