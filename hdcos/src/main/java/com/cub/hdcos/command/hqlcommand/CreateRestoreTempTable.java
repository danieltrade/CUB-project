package com.cub.hdcos.command.hqlcommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.entity.HiveColumnBean;
import com.cub.hdcos.hive.entity.HiveQLBean;
import com.cub.hdcos.hive.entity.HiveQLBean.RowFormat;
import com.cub.hdcos.hive.entity.HiveQLBean.TableType;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.postgres.entity.MetaDataList;

public class CreateRestoreTempTable extends AbstractCubCreateTable {
	
	private String tableName;
	
	private String hdfsPath;
	
	private MetaDataList metaDatas;
	
	public CreateRestoreTempTable(String tableName , String hdfsPath , MetaDataList metaDatas) {
		this.tableName = tableName;
		this.hdfsPath = hdfsPath;
		this.metaDatas = metaDatas;
	}
	
	private HiveQLBean getHiveQLBean() {
		List<ColumninfoList> columnsInfo = metaDatas.getColumns();
		Collections.sort(columnsInfo);
		
		HiveQLBean hqlBean = new HiveQLBean();
		hqlBean.setTableType(TableType.TEMPORARY);
		hqlBean.setTableName(tableName);
		List<HiveColumnBean> columns = new ArrayList<HiveColumnBean>();
		for(ColumninfoList columnInfo : columnsInfo) {
			columns.add(new HiveColumnBean(columnInfo.getColumnName() , columnInfo.getColumnType() , columnInfo.getColumnDesc()));
//			String columnSensitivityCode = columnInfo.getColumnSensitivityCode();
//			if(!"".equals(columnSensitivityCode)) {
//				columns.add(new HiveColumnBean(columnInfo.getColumnName() + "_encode" , columnInfo.getColumnType() , columnInfo.getColumnDesc()));
//			}
		}
		hqlBean.setColumns(columns);
		hqlBean.setRowFormat(RowFormat.SERDE);
		hqlBean.setSerdeClass("'com.cub.hdcos.hive.formater.CubInfoTypeaSerDes'");
		hqlBean.setSerdeProperties(setSerdeProperties(metaDatas));
//		hqlBean.setFileFormat(HiveQLBean.FileFormat.TEXTFILE);
//		hqlBean.setHdfsPath(hdfsPath);
		hqlBean.setInputFormatClass("org.apache.hadoop.mapred.TextInputFormat");
		hqlBean.setOutputFormatClass("com.cub.hdcos.hive.formater.EncodeOutputFormat");
//		hqlBean.setSourceTable(HiveConfig.CUB_DB + "." + tableName.split("[.]")[1]);
		return hqlBean;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.createTableByRowFormatSerde(getHiveQLBean());
	}
	
}
