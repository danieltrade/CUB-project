package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;

public class DropHiveTable implements HqlCommand {
	
	private String tableName;
	
	public DropHiveTable(String tableName) {
		this.tableName = tableName;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.dropTable(tableName);
	}
}
