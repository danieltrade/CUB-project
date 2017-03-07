package com.cub.hdcos.hive.entity;

import java.util.List;
import java.util.Map;

public class HiveQLBean {
	
//	public enum TableType { TEMPORARY("") , EXTERNAL("EXTERNAL");
//		
//		private String tableType;
//		private TableType(String tableType) {
//			this.tableType = tableType;
//		}
//		public String getTableType() {
//			return this.tableType;
//		}
//	}
	public enum TableType { TEMPORARY , EXTERNAL };
	
	private String tableType;
	
	private String tableName;
	
	private List<HiveColumnBean> columns;
	
	private String tableComment;
	
	private List<HiveColumnBean> partitionedColumns;
	
	private List<HiveColumnBean> clusteredColumns;
	
//	public enum RowFormat {
//		DELIMITED("DELIMITED") , SERDE("SERDE");
//		private String rowFormat;
//		private RowFormat(String rowFormat) {
//			this.rowFormat = rowFormat;
//		}
//		public String getRowFormat() {
//			return rowFormat;
//		}
//	};
	public enum RowFormat { DELIMITED , SERDE };
	
	private String rowFormat;
	
	private String serdeClass;
	
	private Map<String,String> serdeProperties;
	
	private String fieldsSplit;
	
	private String collectionItemsSplit;
	
	private String mapKeysSplit;
	
	private String linesSplit;
	
//	public enum FileFormat {
//		SEQUENCEFILE("SEQUENCEFILE"), TEXTFILE("TEXTFILE"), RCFILE("RCFILE"), ORC("ORC"),
//		PARQUET("PARQUET"), AVRO("AVRO"), INPUTFORMAT("INPUTFORMAT"), OUTPUTFORMAT("OUTPUTFORMAT");
//		private String fileFormat;
//		private FileFormat(String fileFormat) {
//			this.fileFormat = fileFormat;
//		}
//		public String getFileFormat() {
//			return this.fileFormat;
//		}
//	};
	public enum FileFormat {
		SEQUENCEFILE, TEXTFILE, RCFILE, ORC, PARQUET, AVRO, INPUTFORMAT, OUTPUTFORMAT
	};
	
	private String fileFormat;
	
	private String hdfsPath;
	
	private String inputFormatClass;
	
	private String outputFormatClass;
	
	private String viewName;
	
	private String sourceTable;
	
	public String getTableType() {
		return tableType;
	}

	public void setTableType(TableType tableType) {
		switch(tableType) {
			case TEMPORARY :
				this.tableType = "";
				break;
			case EXTERNAL :
				this.tableType = "EXTERNAL";
				break;
			default :
				this.tableType = "";
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<HiveColumnBean> getColumns() {
		return columns;
	}

	public void setColumns(List<HiveColumnBean> columns) {
		this.columns = columns;
	}

	public String getTableComment() {
		return tableComment;
	}

	public void setTableComment(String tableComment) {
		this.tableComment = tableComment;
	}

	public List<HiveColumnBean> getPartitionedColumns() {
		return partitionedColumns;
	}

	public void setPartitionedColumns(List<HiveColumnBean> partitionedColumns) {
		this.partitionedColumns = partitionedColumns;
	}

	public List<HiveColumnBean> getClusteredColumns() {
		return clusteredColumns;
	}

	public void setClusteredColumns(List<HiveColumnBean> clusteredColumns) {
		this.clusteredColumns = clusteredColumns;
	}

	public String getRowFormat() {
		return rowFormat;
	}

	public void setRowFormat(RowFormat rowFormat) {
		switch(rowFormat) {
			case DELIMITED :
				this.rowFormat = "DELIMITED";
				break;
			case SERDE :
				this.rowFormat = "SERDE";
				break;
			default :
				this.rowFormat = "";
		}
	}

	public String getSerdeClass() {
		return serdeClass;
	}

	public void setSerdeClass(String serdeClass) {
		this.serdeClass = serdeClass;
	}

	public Map<String, String> getSerdeProperties() {
		return serdeProperties;
	}

	public void setSerdeProperties(Map<String, String> serdeProperties) {
		this.serdeProperties = serdeProperties;
	}

	public String getFieldsSplit() {
		return fieldsSplit;
	}

	public void setFieldsSplit(String fieldsSplit) {
		this.fieldsSplit = fieldsSplit;
	}

	public String getCollectionItemsSplit() {
		return collectionItemsSplit;
	}

	public void setCollectionItemsSplit(String collectionItemsSplit) {
		this.collectionItemsSplit = collectionItemsSplit;
	}

	public String getMapKeysSplit() {
		return mapKeysSplit;
	}

	public void setMapKeysSplit(String mapKeysSplit) {
		this.mapKeysSplit = mapKeysSplit;
	}

	public String getLinesSplit() {
		return linesSplit;
	}

	public void setLinesSplit(String linesSplit) {
		this.linesSplit = linesSplit;
	}

	public String getFileFormat() {
		return fileFormat;
	}

	public void setFileFormat(FileFormat fileFormat) {
		switch(fileFormat) {
			case SEQUENCEFILE :
				this.fileFormat = "SEQUENCEFILE";
				break;
			case TEXTFILE :
				this.fileFormat = "TEXTFILE";
				break;
			case RCFILE :
				this.fileFormat = "RCFILE";
				break;
			case ORC :
				this.fileFormat = "ORC";
				break;
			case PARQUET :
				this.fileFormat = "PARQUET";
				break;
			case AVRO :
				this.fileFormat = "AVRO";
			case INPUTFORMAT :
				this.fileFormat = "INPUTFORMAT";
				break;
			case OUTPUTFORMAT :
				this.fileFormat = "OUTPUTFORMAT";
				break;
			default :
				this.fileFormat = "";
		}
	}

	public String getHdfsPath() {
		return hdfsPath;
	}

	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	public String getInputFormatClass() {
		return inputFormatClass;
	}

	public void setInputFormatClass(String inputFormatClass) {
		this.inputFormatClass = inputFormatClass;
	}

	public String getOutputFormatClass() {
		return outputFormatClass;
	}

	public void setOutputFormatClass(String outputFormatClass) {
		this.outputFormatClass = outputFormatClass;
	}

	public String getViewName() {
		return viewName;
	}

	public void setViewName(String viewName) {
		this.viewName = viewName;
	}

	public String getSourceTable() {
		return sourceTable;
	}

	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}
	
}
