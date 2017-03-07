package com.cub.hdcos.command.hqlcommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.entity.HiveColumnBean;
import com.cub.hdcos.hive.entity.HiveQLBean;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.postgres.entity.MetaDataList;

public class CreateEncryptView implements HqlCommand {
	
	private String viewName;
	
	private MetaDataList metaDatas;
	
	public CreateEncryptView(MetaDataList metaDatas) {
		this.viewName = "";
		this.metaDatas = metaDatas;
	}
	
	public CreateEncryptView(String viewName , MetaDataList metaDatas) {
		this.viewName = viewName;
		this.metaDatas = metaDatas;
	}
	
	private HiveQLBean getHiveQLBean() throws CUBException {
		String tableName = metaDatas.getTableName();
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		List<ColumninfoList> columnsInfo = hiveMetaService.describeTable(tableName);
		Collections.sort(columnsInfo);
		
		List<String> columnsEncrypt = new ArrayList<String>();
		for(int i = 0 ; i < columnsInfo.size() ; i++) {
			ColumninfoList columninfo = columnsInfo.get(i);
			String columnName = columninfo.getColumnName();
			if(columnName.contains("_encode")) {
				columnsEncrypt.remove(i-1);
				columnsInfo.remove(i);
				i--;
			}
			columnsEncrypt.add(columnName + " as " + columnName.replaceAll("_encode", ""));
		}
		
		HiveQLBean hqlBean = new HiveQLBean();
		hqlBean.setViewName(HiveConfig.CUB_ENCRYPTE_DB + "." + tableName);
		if(!"".equals(viewName)) {
			hqlBean.setViewName(viewName);
		}
		
		List<HiveColumnBean> columns = new ArrayList<HiveColumnBean>();
		for(String columnEncrypt : columnsEncrypt) {
			columns.add(new HiveColumnBean(columnEncrypt));
		}
		hqlBean.setColumns(columns);
		hqlBean.setTableName(tableName);
		
		return hqlBean;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.createView(getHiveQLBean());
	}
}
