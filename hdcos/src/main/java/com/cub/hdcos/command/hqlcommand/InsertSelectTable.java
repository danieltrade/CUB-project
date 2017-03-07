package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;

public class InsertSelectTable implements HqlCommand {
	
	private String insertTable;
	
	private String sourceTable;
	
	public InsertSelectTable(String insertTable , String sourceTable) {
		this.insertTable = insertTable;
		this.sourceTable = sourceTable;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.insertSelectTable(insertTable, sourceTable);
	}
}
