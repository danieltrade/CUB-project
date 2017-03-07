package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.hive.jdbc.CubHiveMetaService;

public class LoadTableToHDFS implements HqlCommand {
	
	private String tableName;
	
	private String loadPath;
	
	private char splitChar;
	
	public LoadTableToHDFS(String tableName , String loadPath) {
		this.tableName = tableName;
		this.loadPath = loadPath;
	}
	
	public LoadTableToHDFS(String tableName , char splitChar , String loadPath) {
		this.tableName = tableName;
		this.loadPath = loadPath;
		this.splitChar = splitChar;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		CubHiveMetaService hiveMetaService = new CubHiveMetaService(HiveConfig.DEFAULT_DB);
		if(Character.isWhitespace(splitChar)) {
			hiveMetaService.loadHiveTableIntoHDFS(loadPath, tableName);
		} else {
			hiveMetaService.loadHiveTableIntoHDFS(loadPath, splitChar, tableName);
		}
	}
}
