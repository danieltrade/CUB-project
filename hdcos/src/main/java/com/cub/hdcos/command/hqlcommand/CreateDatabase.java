package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;

public class CreateDatabase implements HqlCommand {
	
	String databaseName;
	
	public CreateDatabase(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.createDatabase(databaseName);
	}

}
