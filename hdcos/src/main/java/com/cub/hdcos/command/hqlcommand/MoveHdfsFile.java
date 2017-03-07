package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hdfs.util.HdfsOperater;

public class MoveHdfsFile implements HqlCommand {
	
	private String srcPath;
	
	private String destPath;
	
	public MoveHdfsFile(String srcPath , String destPath) {
		this.srcPath = srcPath;
		this.destPath = destPath;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		HdfsOperater hdfs = HdfsOperater.getOperater();
		hdfs.moveFile(srcPath, destPath);
		hdfs.clcose();
	}

}
