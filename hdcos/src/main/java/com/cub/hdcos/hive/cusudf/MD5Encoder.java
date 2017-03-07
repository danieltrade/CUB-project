package com.cub.hdcos.hive.cusudf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MD5Encoder extends UDF {
	
	public static void main(String[] args) {
		System.out.println(MD5Encoder.getInstance().evaluate(new Text("abc")));
	}
	
	public static MD5Encoder getInstance() {
		return new MD5Encoder();
	}
	
	public Text evaluate(final Text text) {
		Text encodeStr = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(text.toString().getBytes());
			byte[] encodeByte = md.digest();
			StringBuilder builder = new StringBuilder();
			for(byte b : encodeByte) {
				builder.append(Integer.toString((b & 0xff) + 0x100 , 16).substring(1));
			}
			encodeStr = new Text(builder.toString());
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return encodeStr; 
	}
	
}
