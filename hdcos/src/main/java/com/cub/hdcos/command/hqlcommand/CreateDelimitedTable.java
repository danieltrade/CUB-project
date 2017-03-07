package com.cub.hdcos.command.hqlcommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.entity.HiveColumnBean;
import com.cub.hdcos.hive.entity.HiveQLBean;
import com.cub.hdcos.hive.entity.HiveQLBean.RowFormat;
import com.cub.hdcos.hive.entity.HiveQLBean.TableType;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;
import com.cub.hdcos.hive.serde.properties.CubInfoTypeaSerDesPropConfig;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.postgres.entity.MetaDataList;
import com.cub.hdcos.util.StringUtil;

public class CreateDelimitedTable implements HqlCommand {
	
	private String tableName;
	
	private MetaDataList metaDatas;
	
	public CreateDelimitedTable(String tableName , MetaDataList metaDatas) {
		this.tableName = tableName;
		this.metaDatas = metaDatas;
	}
	
	private HiveQLBean getHiveQLBean() {
		List<ColumninfoList> columnsInfo = metaDatas.getColumns();
		Collections.sort(columnsInfo);
		
		HiveQLBean hqlBean = new HiveQLBean();
		hqlBean.setTableType(TableType.TEMPORARY);
		hqlBean.setTableName(tableName);
		
		StringBuffer inputRegexStr = new StringBuffer();
		StringBuffer outputFormatStr = new StringBuffer();
		String delmiters = metaDatas.getDelmiter();
		List<HiveColumnBean> columns = new ArrayList<HiveColumnBean>();
		
		int columnIndex = 0;
		for(int i = 0 ; i < columnsInfo.size() ; i++) {
			ColumninfoList columnInfo = columnsInfo.get(i);
			columns.add(new HiveColumnBean(columnInfo.getColumnName() , columnInfo.getColumnType() , columnInfo.getColumnDesc()));
			
			inputRegexStr.append("(.*)");
			if(!"".equals(delmiters) && i < (columnsInfo.size() - 1)) {
				for(int j = 0 ; j < delmiters.length() ; j++) {
					String delmiter = String.valueOf(delmiters.charAt(j));
					inputRegexStr.append("\\\\" + delmiter);
				}
			}
			
			outputFormatStr.append("%" + (++columnIndex) +"$s ");
			
			//有加密欄位的要多一個output
			String columnSensitivityCode = columnInfo.getColumnSensitivityCode();
			if(!"".equals(columnSensitivityCode)) {
				columns.add(new HiveColumnBean(columnInfo.getColumnName() + "_encode" , columnInfo.getColumnType() , columnInfo.getColumnDesc()));
				outputFormatStr.append("%" + (++columnIndex) +"$s ");
			}
		}
		
		hqlBean.setColumns(columns);
		hqlBean.setRowFormat(RowFormat.SERDE);
		/*
		hqlBean.setSerdeClass("'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'");
		Map<String,String> serdeProperties = new HashMap<String,String>();
		serdeProperties.put("input.regex", inputRegexStr.toString());
		serdeProperties.put("output.format.string", outputFormatStr.toString());
		hqlBean.setSerdeProperties(serdeProperties);
		*/
		
		return hqlBean;
	}
	
	private Map<String,String> setSerdeProperties() {
		Map<String,String> serdeProperties = new HashMap<String,String>();
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_TABLENAME , metaDatas.getFileName());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_FILEFORMAT, metaDatas.getFileFormat());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_DELMITER, metaDatas.getDelmiter());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_FIXFORMATMAXLENGTH, metaDatas.getFixformatMaxlength());
		List<ColumninfoList> columnsInfo = metaDatas.getColumns();
		Collections.sort(columnsInfo);
		StringBuffer ids = new StringBuffer();
		StringBuffer columnNames = new StringBuffer();
		StringBuffer isUniqueKeys = new StringBuffer();
		StringBuffer columnLengths = new StringBuffer();
		StringBuffer columnFillTypes = new StringBuffer();
		StringBuffer columnFillChars = new StringBuffer();
		StringBuffer columnTypes = new StringBuffer();
		StringBuffer columnDescs = new StringBuffer();
		StringBuffer columnSensitivityCodes = new StringBuffer();
		
		for(int i = 0 ; i < columnsInfo.size() ; i++) {
			if(i != 0) {
				ids.append(",");
				columnNames.append(",");
				isUniqueKeys.append(",");
				columnLengths.append(",");
				columnFillTypes.append(",");
				columnFillChars.append(",");
				columnTypes.append(",");
				columnDescs.append(",");
				columnSensitivityCodes.append(",");
			}
			ColumninfoList columnInfo = columnsInfo.get(i);
			ids.append(StringUtil.parseNullToBlank(columnInfo.getId()));
			columnNames.append(StringUtil.parseNullToBlank(columnInfo.getColumnName()));
			isUniqueKeys.append(StringUtil.parseNullToBlank(columnInfo.getIsUniqueKey()));
			columnLengths.append(StringUtil.parseNullToBlank(columnInfo.getColumnLength()));
			columnFillTypes.append(StringUtil.parseNullToBlank(columnInfo.getColumnFillType()));
			columnFillChars.append(StringUtil.parseNullToBlank(columnInfo.getColumnFillChar()));
			columnTypes.append(StringUtil.parseNullToBlank(columnInfo.getColumnType()));
			columnDescs.append(StringUtil.parseNullToBlank(columnInfo.getColumnDesc()));
			columnSensitivityCodes.append(StringUtil.parseNullToBlank(columnInfo.getColumnSensitivityCode()));
		}
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLMNSID, ids.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNNAME, columnNames.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_ISUNIQUEKEY, isUniqueKeys.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNLENGTH, columnLengths.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNFILLTYPE, columnFillTypes.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNFILLCHAR, columnFillChars.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNTYPE, columnTypes.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNDESC, columnDescs.toString());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_COLUMNSENSITIVITYCODE, columnSensitivityCodes.toString());
		return serdeProperties;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.createTableByRowFormatSerde(getHiveQLBean());
	}
}
