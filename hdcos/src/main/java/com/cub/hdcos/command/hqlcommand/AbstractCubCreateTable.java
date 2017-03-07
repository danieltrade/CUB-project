package com.cub.hdcos.command.hqlcommand;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.hive.serde.properties.CubInfoTypeaSerDesPropConfig;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.postgres.entity.MetaDataList;
import com.cub.hdcos.util.StringUtil;

public abstract class AbstractCubCreateTable implements HqlCommand{
	
	protected Map<String,String> setSerdeProperties(MetaDataList metaDatas) {
		Map<String,String> serdeProperties = new HashMap<String,String>();
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_TABLENAME , metaDatas.getFileName());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_FILEFORMAT, metaDatas.getFileFormat());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_DELMITER, metaDatas.getDelmiter());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_FIXFORMATMAXLENGTH, metaDatas.getFixformatMaxlength());
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_FILE_ENCODING, metaDatas.getFileCode());
		
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
		StringBuffer columnCharTypes = new StringBuffer();
		
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
				columnCharTypes.append(",");
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
			columnCharTypes.append(StringUtil.parseNullToBlank(columnInfo.getColumnCharType()));
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
		serdeProperties.put(CubInfoTypeaSerDesPropConfig.CUB_COLUMNS_CHARTYPE, columnCharTypes.toString());
		return serdeProperties;
	}
	
}
