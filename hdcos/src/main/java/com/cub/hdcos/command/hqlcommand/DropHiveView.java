package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;

public class DropHiveView implements HqlCommand {
	
	private String viewName;
	
	public DropHiveView(String viewName) {
		this.viewName = viewName;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		hiveMetaService.dropView(viewName);
	}
}
