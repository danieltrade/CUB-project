package com.cub.hdcos.shell.execute;

import com.cub.hdcos.command.MultiHdcosCommand;
import com.cub.hdcos.command.hqlcommand.CreateDatabase;
import com.cub.hdcos.command.hqlcommand.CreateRestoreTempTable;
import com.cub.hdcos.command.hqlcommand.DropHiveTable;
import com.cub.hdcos.command.hqlcommand.InsertSelectTable;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.postgres.config.PosgresConfig;
import com.cub.hdcos.postgres.entity.MetaDataList;
import com.cub.hdcos.postgres.service.MetaInfoService;

public class HiveTableRestoreForSqoopShell {

	//command {tablename} {hdfspath}
	//Because the "origin" file is exist in /user/hive/warehouse, not need to do things
	public static void main(String[] args) {
		try{
			//tableName -> DP_MCIF__ACCT_2JC_LGD906
			String tableName = args[0];
//			String hdfsPath = args[1];
			MetaInfoService serviec = MetaInfoService.getInstance(PosgresConfig.DEFAULT_DB);
			MetaDataList metaDatas = serviec.queryMetaDataList(tableName);
			String sourceTable = tableName;
			String insertTable = HiveConfig.CUB_RESTORE_DB + "." + tableName;
			MultiHdcosCommand multiHdcosCommand = new MultiHdcosCommand();
			multiHdcosCommand.addCommand(new DropHiveTable(insertTable));
			multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_RESTORE_DB));
			multiHdcosCommand.addCommand(new CreateRestoreTempTable(insertTable , HiveConfig.RESTORE_HDFS_TEMP_PATH + tableName , metaDatas));
			multiHdcosCommand.addCommand(new InsertSelectTable(insertTable, sourceTable));
			multiHdcosCommand.execute();
			
			/*
			multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_RESTORE_DB));
			multiHdcosCommand.addCommand(new CreateRestoreTempTable(tempTableName , HiveConfig.RESTORE_HDFS_TEMP_PATH + tableName , metaDatas));
			multiHdcosCommand.addCommand(new InsertSelectTable(tempTableName, HiveConfig.CUB_DB + "." + tableName));
			multiHdcosCommand.addCommand(new DownladHdfsFileToLocal(true , downloadPath, localPath));
			multiHdcosCommand.addCommand(new DropHiveTable(tempTableName));
			multiHdcosCommand.addCommand(new RenameHiveTempFile(localPath , localFilePath));
			*/
		} catch(CUBException e) {
			e.printStackTrace();
		}
	}
}
