package com.cub.encrypt.entity;

public class EncodeBean {
	
	private String sensitivityCode;
	
	private String encodeType;
	
	private String encodeMethod;
	
	public EncodeBean() {
	}
	
	public EncodeBean(String sensitivityCode , String encodeType , String encodeMethod) {
		this.sensitivityCode = sensitivityCode;
		this.encodeType = encodeType;
		this.encodeMethod = encodeMethod;
	}

	public String getSensitivityCode() {
		return sensitivityCode;
	}

	public void setSensitivityCode(String sensitivityCode) {
		this.sensitivityCode = sensitivityCode;
	}

	public String getEncodeType() {
		return encodeType;
	}

	public void setEncodeType(String encodeType) {
		this.encodeType = encodeType;
	}

	public String getEncodeMethod() {
		return encodeMethod;
	}

	public void setEncodeMethod(String encodeMethod) {
		this.encodeMethod = encodeMethod;
	}
	
}
