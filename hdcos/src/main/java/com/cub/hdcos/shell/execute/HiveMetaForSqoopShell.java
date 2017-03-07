package com.cub.hdcos.shell.execute;


import com.cub.hdcos.command.MultiHdcosCommand;
import com.cub.hdcos.command.hqlcommand.CreateDatabase;
import com.cub.hdcos.command.hqlcommand.CreateEncryptView;
import com.cub.hdcos.command.hqlcommand.CreateFixFormatTable;
import com.cub.hdcos.command.hqlcommand.CreateView;
import com.cub.hdcos.command.hqlcommand.DropHiveTable;
import com.cub.hdcos.command.hqlcommand.DropHiveView;
import com.cub.hdcos.command.hqlcommand.LoadHDFSFileIntoTable;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hdfs.util.HdfsOperater;
import com.cub.hdcos.hive.config.HiveConfig;
import com.cub.hdcos.postgres.config.PosgresConfig;
import com.cub.hdcos.postgres.entity.MetaDataList;
import com.cub.hdcos.postgres.service.MetaInfoService;
public class HiveMetaForSqoopShell {
	
	//command {tableName} {hdfsPath}
	public static void main(String[] args) {
		try {
			String tableName = args[0];
			String outPath = args[1];
			
			MetaInfoService serviec = MetaInfoService.getInstance(PosgresConfig.DEFAULT_DB);
			MetaDataList metaDatas = serviec.queryMetaDataList(tableName);
			
			HdfsOperater operater = HdfsOperater.getOperater();
			MultiHdcosCommand multiHdcosCommand = new MultiHdcosCommand();
			multiHdcosCommand.addCommand(new DropHiveTable(tableName));
			multiHdcosCommand.addCommand(new CreateFixFormatTable(tableName , metaDatas));
			multiHdcosCommand.addCommand(new LoadHDFSFileIntoTable(outPath , tableName));
			multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_DB));
			multiHdcosCommand.addCommand(new CreateDatabase(HiveConfig.CUB_ENCRYPTE_DB));
			multiHdcosCommand.addCommand(new DropHiveView(HiveConfig.CUB_DB + "." + tableName));
			multiHdcosCommand.addCommand(new CreateView(metaDatas));
			multiHdcosCommand.addCommand(new DropHiveView(HiveConfig.CUB_ENCRYPTE_DB + "." + tableName));
			multiHdcosCommand.addCommand(new CreateEncryptView(metaDatas));
			multiHdcosCommand.execute();
			operater.deleteDirectory(outPath, true);
		} catch(CUBException e) {
			e.printStackTrace();
		}
		
	}
}
