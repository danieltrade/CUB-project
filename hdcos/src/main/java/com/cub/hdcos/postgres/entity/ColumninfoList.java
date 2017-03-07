package com.cub.hdcos.postgres.entity;

public class ColumninfoList implements Comparable<ColumninfoList> {
	
	private String id;
	
	private String fileName;
	
	private String columnName;
	
	private String isUniqueKey;
	
	private String columnLength;
	
	private String columnFillType;
	
	private String columnFillChar;
	
	private String columnType;
	
	private String columnDesc;
	
	private String columnSensitivityCode;
	
	private String columnCharType;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getIsUniqueKey() {
		return isUniqueKey;
	}

	public void setIsUniqueKey(String isUniqueKey) {
		this.isUniqueKey = isUniqueKey;
	}

	public String getColumnLength() {
		return columnLength;
	}

	public void setColumnLength(String columnLength) {
		this.columnLength = columnLength;
	}

	public String getColumnFillType() {
		return columnFillType;
	}

	public void setColumnFillType(String columnFillType) {
		this.columnFillType = columnFillType;
	}

	public String getColumnFillChar() {
		return columnFillChar;
	}

	public void setColumnFillChar(String columnFillChar) {
		this.columnFillChar = columnFillChar;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public String getColumnDesc() {
		return columnDesc;
	}

	public void setColumnDesc(String columnDesc) {
		this.columnDesc = columnDesc;
	}

	public String getColumnSensitivityCode() {
		return columnSensitivityCode;
	}

	public void setColumnSensitivityCode(String columnSensitivityCode) {
		this.columnSensitivityCode = columnSensitivityCode;
	}

	public String getColumnCharType() {
		return columnCharType;
	}

	public void setColumnCharType(String columnCharType) {
		this.columnCharType = columnCharType;
	}

	@Override
	public int compareTo(ColumninfoList column) {
		if(this.id == null) {
			this.id = "0";
		}
		if(column.getId() == null) {
			column.setId("0");
		}
		int thisIdVal = Integer.parseInt(this.id);
		int compIdVal = Integer.parseInt(column.getId());
		return thisIdVal - compIdVal;
	}
}
