package com.cub.hdcos.hive.formater;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.cub.encrypt.service.CubEncryptUtil;
import com.cub.hdcos.common.type.ColumnFillType;
import com.cub.hdcos.common.type.FileformatType;
import com.cub.hdcos.hive.serde.properties.CubInfoTypeaSerDesPropConfig;
import com.cub.hdcos.util.StringUtil;
//com.cub.hdcos.hive.formater.CubInfoTypeaSerDes
//org.apache.hadoop.hive.serde2.RegexSerDe
@SerDeSpec(schemaProps={"cub.tableName","cub.fileFormat","cub.delmiter","cub.fixformatMaxlength","cub.columns.id",
"cub.columns.columnName","cub.columns.isUniqueKey","cub.columns.columnLength","cub.columns.columnFillType",
"cub.columns.columnFillChar","cub.columns.columnType","cub.columns.columnDesc","cub.columns.columnSensitivityCode",
"cub.file.encoding","cub.columns.charType"})
public class CubInfoTypeaSerDes extends AbstractSerDe {
	
	final static Logger logger = Logger.getLogger(CubInfoTypeaSerDes.class);
	
	protected StructTypeInfo rowTypeInfo;
	protected ObjectInspector rowOI;
	protected List<String> colNames;
	protected List<Object> row = new ArrayList<Object>();
	protected List<TypeInfo> colTypes;
	
	protected String dbName;
	protected String tableName;
	
	private String[] outputFields;
	private int numCols;
	
	protected LazySerDeParameters serdeParams;
	protected LazySimpleSerDe lazySimpleSerDe;
	
	private long deserializedByteCount;
	private SerDeStats stats;
	
	private Properties serdeProp;
	
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		lazySimpleSerDe = new LazySimpleSerDe();
		String encoding = tbl.getProperty(CubInfoTypeaSerDesPropConfig.CUB_FILE_ENCODING);
		tbl.setProperty("serialization.encoding", encoding);
		lazySimpleSerDe.initialize(conf, tbl);
//		Properties tbl :
//		key : name , value : default.cubinfotypea
//		key : columns.types , value : string:string:string:string:string
//		key : serialization.ddl , value : struct cubinfotypea { string id, string name, string tel, string addr, string team}
//		key : serialization.format , value : 1
//		key : columns , value : id,name,tel,addr,team
//		key : columns.comments , value : peapleidpeaplenamepaopletel地址隊代碼
//		key : bucket_count , value : -1
//		key : serialization.lib , value : com.cub.hdcos.hive.formater.CubInfoTypeaSerDes
//		key : file.inputformat , value : org.apache.hadoop.mapred.TextInputFormat
//		key : file.outputformat , value : org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
		
		serdeParams = new LazySerDeParameters(conf, tbl, getClass().getName());
		this.serdeProp = tbl;
		
		// Get a list of the table's column names.
		String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
		colNames = Arrays.asList(colNamesStr.split(","));
		// Get a list of TypeInfos for the columns. This list lines up with the list of column names.
		String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
		colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
		rowTypeInfo =  (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
		
		String name = tbl.getProperty("name");//key : name , value : default.cubinfotypea
		dbName = name.split("[.]")[0];
		tableName = name.split("[.]")[1];
		
		this.outputFields = new String[numCols];
		stats = new SerDeStats();
		deserializedByteCount = 0;
	}
	/**
	  * This method does the work of deserializing a record into Java objects
	  * that Hive can work with via the ObjectInspector interface.
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		row.clear();
		Text rowData = (Text)blob;
		String encoding = serdeProp.getProperty(CubInfoTypeaSerDesPropConfig.CUB_FILE_ENCODING);
		String rowDataStr = setRowDataEncoding(encoding , rowData);
//		String rowDataStr = rowData.toString();
		
		String fileFormat = serdeProp.getProperty(CubInfoTypeaSerDesPropConfig.CUB_FILEFORMAT);
		if(FileformatType.DELIMITED.equals(fileFormat)) {
			String delmiters = serdeProp.getProperty(CubInfoTypeaSerDesPropConfig.CUB_DELMITER);
			String[] columnSensitivityCodes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNSENSITIVITYCODE)).split(",");
			//columns 實際使用者需要的欄位(無加密欄位):name,birthday,addr,tel,cellphone,email
			String[] columns = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNNAME)).split(",");
			String[] rowdatas = rowDataStr.split(getDelimiterSplit(delmiters));
			String[] columnTypes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNTYPE)).split(",");
			String[] columnChartypes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_CHARTYPE)).split(",");
			
			Map<String , String> tempColumnDatas = new HashMap<String ,String>();
			Map<String , String> tempSensitivityCodes = new HashMap<String ,String>();
			Map<String , String> tempChartypes = new HashMap<String ,String>();
			for(int i = 0 ; i < rowdatas.length ; i++) {
				String tempRowData = rowdatas[i];
				String tempKey = columns[i].toLowerCase().trim();
				String colChartype = columnChartypes[i].trim();
				if(!"".equals(colChartype) && !"".equals(tempRowData)) {
					tempChartypes.put(tempKey, colChartype);
					tempRowData = setRowDataEncoding(colChartype , new Text(tempRowData));
				}
				tempColumnDatas.put(tempKey, tempRowData);
				String sensitivityCode = columnSensitivityCodes[i].trim();
				if(!"".equals(sensitivityCode)) {
					//要加密的欄位，先把加密方式存起來
					tempSensitivityCodes.put(tempKey, sensitivityCode);
				}
				
			}
			//colNames table實際的column(有加密欄位):name,name_encode,birthday,addr,addr_encode,tel,cellphone,email
			for(int i = 0 ; i < colNames.size() ; i++) {
				String colName = colNames.get(i);
				String colVal = StringUtil.parseNull(tempColumnDatas.get(colName.replaceAll("_encode", "")));
				String colType = colTypes.get(i).getTypeName();
				String chartype = StringUtil.parseNull(tempChartypes.get(colName.replaceAll("_encode", "")));
				if(!"".equals(chartype.trim()) && !"".equals(colVal)) {
					try {
						colVal = convertChartype(colVal, chartype);
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
						throw new SerDeException("convertChartype " + chartype + " value is " + colVal + "error ! ");
					} catch (DecoderException e) {
						e.printStackTrace();
						throw new SerDeException("convertChartype " + chartype + " value is " + colVal + "error ! ");
					}
				}
				if(colName.contains("_encode")) {
					colVal = tempColumnDatas.get(colName.replaceAll("_encode", ""));
					String sensitivityCode = tempSensitivityCodes.get(colName.replaceAll("_encode", ""));
					try {
						colVal = CubEncryptUtil.getInstance(sensitivityCode).encodeValue(colVal);
					} catch (Exception e) {
						e.printStackTrace();
						throw new SerDeException("CubEncryptUtil encode columnName '" + colName + "' failed , this value is " + colVal + " sensitivityCode is " + sensitivityCode + " ! ");
					}
				}
				
				Object rowDataObj = convertColumnType(colType, colVal);
				row.add(rowDataObj);
			}
			return row;
		}
		
		if(FileformatType.DELIMITED_FIXFORMAT.equals(fileFormat)) {
			String delmiters = serdeProp.getProperty(CubInfoTypeaSerDesPropConfig.CUB_DELMITER);
			rowDataStr = rowDataStr.replaceAll(getDelimiterSplit(delmiters) , "");
		}
		
		String[] columns = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNNAME)).split(",");
		String[] columnLengths = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNLENGTH)).split(",");
		String[] columnSensitivityCodes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNSENSITIVITYCODE)).split(",");
		String[] columnFillTypes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNFILLTYPE)).split(",");
		String[] columnFillChars = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNFILLCHAR)).split(",");
		String[] columnTypes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNTYPE)).split(",");
		String[] columnChartypes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_CHARTYPE)).split(",");
		
		
		int startIndex = 0;
		for(int i = 0 ; i < columns.length ; i++) {
			int columnLength = Integer.parseInt(columnLengths[i]);
			int endIndex = startIndex + columnLength;
			String columnName = columns[i];
			String columnValue = rowDataStr.substring(startIndex , endIndex);
			String fillType = columnFillTypes[i];
			String fillChar = columnFillChars[i];
			String colChartype = columnChartypes[i];
			
			if(!"".equals(colChartype.trim()) && !"".equals(columnValue)) {
				try {
					columnValue = convertChartype(columnValue, colChartype);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
					throw new SerDeException("convertChartype " + colChartype + " value is " + columnValue + "error ! ");
				} catch (DecoderException e) {
					e.printStackTrace();
					throw new SerDeException("convertChartype " + colChartype + " value is " + columnValue + "error ! ");
				}
			}
			
			if(ColumnFillType.RIGHT.equals(fillType)) {
				columnValue = replaceRightChar(columnValue, fillChar);
			} else if(ColumnFillType.LEFT.equals(fillType)) {
				columnValue = replaceLeftChar(columnValue, fillChar);
			}
			String colType = columnTypes[i];
			Object rowDataObj = convertColumnType(colType, columnValue);
			row.add(rowDataObj);
			
			String sensitivityCode = columnSensitivityCodes[i].trim();
			if(!"".equals(sensitivityCode)) {
				try {
					columnValue = CubEncryptUtil.getInstance(sensitivityCode).encodeValue(columnValue);
				} catch (Exception e) {
					e.printStackTrace();
					throw new SerDeException("CubEncryptUtil encode columnName '" + columnName + "' failed , this value is " + columnValue + " sensitivityCode is " + sensitivityCode + " ! ");
				}
				rowDataObj = convertColumnType(colType, columnValue);
				row.add(rowDataObj);
			}
			
			startIndex = endIndex;
		}
		
		return row;
	}
	private String convertChartype(String colVal, String chartype) throws DecoderException, UnsupportedEncodingException {
		byte[] bytes = Hex.decodeHex(colVal.toCharArray());
		if("2".equals(chartype)) {
			colVal = new String(bytes, "utf-8");
		} else {
			colVal = new String(bytes, "big5");
		}
		return colVal;
	}
	
	private Object convertColumnType(String columnType , String columnValue) {
		Object obj = null;
		columnType = columnType.toLowerCase();
		if("string".equals(columnType)) {
			obj = String.valueOf(columnValue);
		} else if("int".equals(columnType)) {
			obj = Integer.parseInt(columnValue);
		} else if("float".equals(columnType)) {
			obj = Float.parseFloat(columnValue);
		}
//		else if("date".equals(columnType)) {
			//yyyy-MM-dd
//			obj = new Date(System.currentTimeMillis());
//		}
		return obj;
	}

	/**
	  * Return an ObjectInspector for the row of data
	  */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}
	
	/**
	  * Unimplemented
	  */
	@Override
	public SerDeStats getSerDeStats() {
		stats.setRawDataSize(deserializedByteCount);
	    return stats;
	}

	/**
	  * Return the class that stores the serialized data representation.
	  */
	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	/**
	  * This method takes an object representing a row of data from Hive, and
	  * uses the ObjectInspector to get the data for each column and serialize
	  * it.
	  */
	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)  throws SerDeException {
		StringBuilder builder = new StringBuilder();
		StructObjectInspector structOI =  (StructObjectInspector) objInspector;
		List<? extends StructField> structFields = structOI.getAllStructFieldRefs();
		//select 未加密的view insert
		String[] columns = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNNAME)).split(",");
		String[] columnLengths = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNLENGTH)).split(",");
		String[] columnFillTypes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNFILLTYPE)).split(",");
		String[] columnFillChars = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNFILLCHAR)).split(",");
		String delmiters = serdeProp.getProperty(CubInfoTypeaSerDesPropConfig.CUB_DELMITER);
		String fileFormat = serdeProp.getProperty(CubInfoTypeaSerDesPropConfig.CUB_FILEFORMAT);
//		String[] columnEncodes = StringUtil.parseNull(serdeProp.get(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_ENCODE)).split(",");
		
		for (int i = 0; i < structFields.size(); i++) {
			if(!"".equals(delmiters) && i > 0) {
				builder.append(delmiters);
			}
			String fieldContent = "";
			StructField structField = structFields.get(i);
			Object fieldData = structOI.getStructFieldData(obj, structField);
			ObjectInspector fieldObjectInspector = structField.getFieldObjectInspector();
//			System.out.println("Object real class name : " + fieldObjectInspector.getClass().getName());
			if(fieldObjectInspector instanceof JavaIntObjectInspector ) {
				JavaIntObjectInspector fieldOI = (JavaIntObjectInspector) fieldObjectInspector;
				fieldContent = String.valueOf(fieldOI.getPrimitiveJavaObject(fieldData));
			} else if(fieldObjectInspector instanceof StringObjectInspector ) {
				StringObjectInspector fieldOI = (StringObjectInspector)fieldObjectInspector;
				fieldContent = fieldOI.getPrimitiveJavaObject(fieldData);
			} else if(fieldObjectInspector instanceof JavaFloatObjectInspector ) {
				JavaFloatObjectInspector fieldOI = (JavaFloatObjectInspector) fieldObjectInspector;
				fieldContent = String.valueOf(fieldOI.getPrimitiveJavaObject(fieldData));
			}
//			else if(fieldObjectInspector instanceof JavaDateObjectInspector ) {
//				JavaDateObjectInspector fieldOI = (JavaDateObjectInspector) fieldObjectInspector;
//				fieldContent = String.valueOf(fieldOI.getPrimitiveJavaObject(fieldData));
//			}
			
			int columnLength = Integer.parseInt(columnLengths[i]);
			String columnFillType = columnFillTypes[i];
			String columnFillChar = columnFillChars[i];
			
			if (fieldContent != null) {
				if(!FileformatType.DELIMITED.equals(fileFormat) && fieldContent.trim().length() != columnLength) {
					if(ColumnFillType.RIGHT.equals(columnFillType)) {
						fieldContent = StringUtils.rightPad(fieldContent.trim() , columnLength , columnFillChar);
					} else if(ColumnFillType.LEFT.equals(columnFillType)) {
						fieldContent = StringUtils.leftPad(fieldContent.trim() , columnLength , columnFillChar);
					}
				}
				builder.append(fieldContent);
			}
		}
		
		return new Text(builder.toString());
	}
	
	private String setRowDataEncoding(String encoding , Text rowData) {
		String rowDataStr = rowData.toString();
		try {
			rowDataStr = new String(rowData.getBytes(), 0, rowData.getLength(), encoding);
			logger.debug("#### USE " + encoding + " encoding rowDataStr value is : " + rowDataStr);
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage() , e);
		}
		return rowDataStr;
	}
	
	private String getDelimiterSplit(String delmiters) {
		StringBuffer inputRegexStr = new StringBuffer();
		if(!"".equals(delmiters)) {
			for(int i = 0 ; i < delmiters.length() ; i++) {
				String delmiter = String.valueOf(delmiters.charAt(i));
				if("\\".equals(delmiter)) {
					continue;
				}
				inputRegexStr.append("\\" + delmiter);
			}
		}
		return inputRegexStr.toString();
	}
	
	private static String replaceLeftChar(String context , String splitchar) {
		String retStr = "";
		int splitCharLength = splitchar.length();
		int startIndex = 0;
		for(int i = 0 ; i < context.length() ; i += splitCharLength) {
			String temp = context.substring(i , (i + splitCharLength));
			if(splitchar.equals(temp)) {
				continue;
			} else {
				startIndex = i;
				break;
			}
		}
		retStr = context.substring(startIndex);
		return retStr;
	}
	
	private static String replaceRightChar(String context , String splitchar) {
		String retStr = "";
		int endindex = 0;
		int splitCharLength = splitchar.length();
		for(int i = (context.length()) ; i > 0 ; i -= splitCharLength) {
			String temp = context.substring((i - splitCharLength), i);
			if(splitchar.equals(temp)) {
				continue;
			} else {
				endindex = i;
				break;
			}
		}
		retStr = context.substring(0, endindex);
		return retStr;
	}
}
