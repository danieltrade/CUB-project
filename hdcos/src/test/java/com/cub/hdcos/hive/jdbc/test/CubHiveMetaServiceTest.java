package com.cub.hdcos.hive.jdbc.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.entity.HiveColumnBean;
import com.cub.hdcos.hive.entity.HiveQLBean;
import com.cub.hdcos.hive.entity.HiveQLBean.FileFormat;
import com.cub.hdcos.hive.entity.HiveQLBean.RowFormat;
import com.cub.hdcos.hive.entity.HiveQLBean.TableType;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;
import com.cub.hdcos.postgres.entity.ColumninfoList;

public class CubHiveMetaServiceTest {
	
	CubHiveMetaService hiveService;
	@Before
	public void init() {
		hiveService = CubHiveMetaService.getInstance(HiveConfig.DEFAULT_DB);
	}
	
	@Test
	public void createTableByRowFormatDelimitedTest() {
		HiveQLBean hqlBean = new HiveQLBean();
		hqlBean.setTableType(TableType.TEMPORARY);
		hqlBean.setTableName("employees");
		List<HiveColumnBean> columns = new ArrayList<HiveColumnBean>();
		columns.add(new HiveColumnBean("name" , "string" , "enployees name"));
		columns.add(new HiveColumnBean("salary" , "float" , "enployees salary"));
		columns.add(new HiveColumnBean("subordinates" , "array<string>" , "enployees subordinates"));
		columns.add(new HiveColumnBean("deductions" , "map<string,float>" , "enployees deductions"));
		columns.add(new HiveColumnBean("address" , "struct<street:string, city:string, state:string, zip:int>" , "enployees address"));
		hqlBean.setColumns(columns);
		hqlBean.setRowFormat(RowFormat.DELIMITED);
		hqlBean.setFieldsSplit("\\001");
		hqlBean.setCollectionItemsSplit("\\002");
		hqlBean.setMapKeysSplit("\\003");
		hqlBean.setLinesSplit("\\n");
		hqlBean.setFileFormat(FileFormat.TEXTFILE);
		
		try {
			hiveService.createTableByRowFormatDelimited(hqlBean);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void createTableByRowFormatSerdeTest() {
		//create external table if not exists AAAFN1 (id string,name_encode string,gender string,age string,addr string) row format SERDE 'com.cub.hdcos.hive.formater.EncodeSerDes' location '/test';
		HiveQLBean hqlBean = new HiveQLBean();
		hqlBean.setTableType(TableType.EXTERNAL);
		hqlBean.setTableName("AAAFN1");
		List<HiveColumnBean> columns = new ArrayList<HiveColumnBean>();
		columns.add(new HiveColumnBean("id" , "string" , "id"));
		columns.add(new HiveColumnBean("name_encode" , "string" , "name_encode"));
		columns.add(new HiveColumnBean("gender" , "string" , "gender"));
		columns.add(new HiveColumnBean("addr" , "string" , "addr"));
		hqlBean.setColumns(columns);
		hqlBean.setRowFormat(RowFormat.SERDE);
		hqlBean.setSerdeClass("'com.cub.hdcos.hive.formater.EncodeSerDes'");
		hqlBean.setHdfsPath("'/test'");
		try {
			hiveService.createTableByRowFormatSerde(hqlBean);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void dropTableTest() {
		String tableName = "AAAFN1";
		try {
			hiveService.dropTable(tableName);
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void showTablesTest() {
		List<String> tables = new ArrayList<String>();
		try {
			tables = hiveService.showTables();
		} catch (CUBException e) {
			e.printStackTrace();
		}
		for(String table : tables) {
			System.out.println(table);
		}
	}
	@Test
	public void describeTableTest() {
		try{
			String tableName = "AAAFN1";
			List<ColumninfoList> schema = hiveService.describeTable(tableName);
			for(ColumninfoList column : schema) {
				System.out.println(column.getColumnName() + " , " + column.getColumnType() + " , " + column.getColumnDesc());
			}
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
	@Test
	public void queryTableTest() {
		try{
			String tableName = "AAAFN1";
			String[] columns = {"id" , "name_encode" , "gender" , "addr"};
			List<Map<String,Object>> datas = hiveService.queryTable(tableName, columns);
			for(Map<String,Object> data : datas) {
				for(String column : columns) {
					System.out.println(data.get(column));
				}
			}
		} catch (CUBException e) {
			e.printStackTrace();
		}
	}
}
