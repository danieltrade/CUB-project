package com.cub.hdcos.command.hqlcommand;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hdfs.util.HdfsOperater;

public class DownladHdfsFileToLocal implements HqlCommand {
	
	private String downloadPath;
	
	private String localPath;
	
	private boolean deleteHdfsFile;
	
	public DownladHdfsFileToLocal(boolean deleteHdfsFile , String downloadPath , String localPath) {
		this.downloadPath = downloadPath;
		this.localPath = localPath;
		this.deleteHdfsFile = deleteHdfsFile;
	}
	
	@Override
	public void executeHQL() throws CUBException {
		HdfsOperater hdfs = HdfsOperater.getOperater();
		hdfs.downladHdfsFileToLocal(deleteHdfsFile , downloadPath, localPath);
		hdfs.clcose();
	}
}
