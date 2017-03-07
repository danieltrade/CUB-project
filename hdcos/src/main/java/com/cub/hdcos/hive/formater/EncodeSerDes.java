package com.cub.hdcos.hive.formater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.cub.encrypt.jnicall.service.EncryptService;


public class EncodeSerDes extends AbstractSerDe {
	
	protected StructTypeInfo rowTypeInfo;
	protected ObjectInspector rowOI;
	protected List<String> colNames;
	protected List<Object> row = new ArrayList<Object>();
	protected List<TypeInfo> colTypes;
	@Override
	public void initialize(Configuration conf, Properties tbl) throws SerDeException {
		// Get a list of the table's column names.
		String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
		colNames = Arrays.asList(colNamesStr.split(","));
		// Get a list of TypeInfos for the columns. This list lines up with the list of column names.
		String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
		colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
		rowTypeInfo =  (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
		rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	}

	/**
	  * This method does the work of deserializing a record into Java objects
	  * that Hive can work with via the ObjectInspector interface.
	 */
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		row.clear();

		Text rowData = (Text)blob;
		String rowDataStr = rowData.toString();
		String[] rds = rowDataStr.split(",");

		for (int i = 0 ; i < colNames.size() ; i++) {
			TypeInfo typeInfo = colTypes.get(i);
			String colName = colNames.get(i);
			String colVal = rds[i];
			//可判斷colName是否含"_encode"有的話就加密
			if(colName.contains("_encode")) {
				//colVal = MD5Encoder.getInstance().evaluate(new Text(colVal)).toString();
				EncryptService service = new EncryptService();
				colVal = service.encodeValue(colVal);
			}
			row.add(colVal);
		}
		
		return row;
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
		return null;
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
	public Writable serialize(Object obj, ObjectInspector oi)  throws SerDeException {
		// Take the object and transform it into a serialized representation
		return new Text();
	}
}