package com.cub.hdcos.exception;

public class CUBException extends Exception {

	private static final long serialVersionUID = -7740168650944215478L;
	
	private String errorCode;
	
	private String errorClassName;

	public CUBException() {
		super();
	}

	public CUBException(String message) {
		super(message);
	}
	
	public CUBException(String message , String errorClassName) {
		super(message);
		this.errorClassName = errorClassName;
	}
	
	public CUBException(String message , String errorCode , String errorClassName) {
		super(message);
		this.errorCode = errorCode;
		this.errorClassName = errorClassName;
	}
	
	public CUBException(String message, Throwable cause) {
		super(message, cause);
	}

	public CUBException(Throwable cause) {
		super(cause);
	}

	public String getErrorCode() {
		return errorCode;
	}

	public String getErrorClassName() {
		return errorClassName;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public void setErrorClassName(String errorClassName) {
		this.errorClassName = errorClassName;
	}

}
