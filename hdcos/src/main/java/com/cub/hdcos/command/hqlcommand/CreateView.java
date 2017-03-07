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

public class CreateView implements HqlCommand {
	
	private String viewName;
	
	private MetaDataList metaDatas;
	
	public CreateView(MetaDataList metaDatas) {
		this.viewName = "";
		this.metaDatas = metaDatas;
	}
	
	public CreateView(String viewName , MetaDataList metaDatas) {
		this.viewName = viewName;
		this.metaDatas = metaDatas;
	}
	
	private HiveQLBean getHiveQLBean() {
		List<ColumninfoList> columnsInfo = metaDatas.getColumns();
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
			columnsEncrypt.add(columnName);
		}
		
		HiveQLBean hqlBean = new HiveQLBean();
		hqlBean.setViewName(HiveConfig.CUB_DB + "." + metaDatas.getTableName());
		if(!"".equals(viewName)) {
			hqlBean.setViewName(viewName);
		}
		List<HiveColumnBean> columns = new ArrayList<HiveColumnBean>();
		for(ColumninfoList columnInfo : columnsInfo) {
			columns.add(new HiveColumnBean(columnInfo.getColumnName()));
		}
		hqlBean.setColumns(columns);
		hqlBean.setTableName(metaDatas.getTableName());
		
		return hqlBean;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.createView(getHiveQLBean());
	}
}
