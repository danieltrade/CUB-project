package com.cub.hdcos.postgres.entity;

import java.util.List;

public class MetaDataList {
	
	private String tableName;
	
	private String fileName;
	//1 : delimited ; 1 : fixformat ; 2 : both
	private String fileFormat;
	//max 5 chars
	private String delmiter;
	
	private String fixformatMaxlength;
	
	private String fileCode;
	
	private List<ColumninfoList> columns;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileFormat() {
		return fileFormat;
	}

	public void setFileFormat(String fileFormat) {
		this.fileFormat = fileFormat;
	}

	public String getDelmiter() {
		return delmiter;
	}

	public void setDelmiter(String delmiter) {
		this.delmiter = delmiter;
	}

	public String getFixformatMaxlength() {
		return fixformatMaxlength;
	}

	public void setFixformatMaxlength(String fixformatMaxlength) {
		this.fixformatMaxlength = fixformatMaxlength;
	}

	public String getFileCode() {
		return fileCode;
	}

	public void setFileCode(String fileCode) {
		this.fileCode = fileCode;
	}

	public List<ColumninfoList> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumninfoList> columns) {
		this.columns = columns;
	}
	
}
