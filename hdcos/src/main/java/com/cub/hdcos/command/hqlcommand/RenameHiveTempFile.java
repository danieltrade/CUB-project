package com.cub.hdcos.command.hqlcommand;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cub.hdcos.command.HqlCommand;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.util.CubFileUtil;

public class RenameHiveTempFile implements HqlCommand {
	
	final static Logger logger = Logger.getLogger(RenameHiveTempFile.class);

	private String oldFilePath;

	private String newFilePath;
	
	private List<String> delFiles;

	public RenameHiveTempFile(String oldFilePath, String newFilePath) {
		this.oldFilePath = oldFilePath;
		this.newFilePath = newFilePath;
	}
	
	private void mergeTempFile() {
		StringBuffer linuxCommand = new StringBuffer();
		
		File dir = new File(oldFilePath);
		String tempFileName = "";
		if(dir.exists() && dir.isDirectory()) {
			File[] files = dir.listFiles();
			if(files != null) {
				delFiles = new ArrayList<String>();
				linuxCommand.append("cat ");
				for(File file : files) {
					String fileName = file.getName();
					if(file.isHidden() || fileName.contains(".crc")) {
						boolean delSucess = CubFileUtil.deleteDir(file);
						continue;
					}
					delFiles.add(oldFilePath +  "/" + fileName);
					linuxCommand.append(oldFilePath +  "/" + fileName + " ");
				}
				linuxCommand.append(" > ");
				linuxCommand.append(newFilePath);
			}
		}
		
		if(!"".equals(linuxCommand.toString())) {
			System.out.println("Execute linux command : " + linuxCommand.toString());
			executeCommand(linuxCommand.toString());
		}
	}
	private String executeCommand(String command) {
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader =  new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();
	}

	@Override
	public void executeHQL() throws CUBException {
		File dir = new File(oldFilePath);
		String tempFileName = "";
		if(dir.exists() && dir.isDirectory()) {
			File[] files = dir.listFiles();
			if(files != null) {
				for(File file : files) {
					String fileName = file.getName();
					if(file.isHidden() || fileName.contains(".crc")) {
						boolean delSucess = CubFileUtil.deleteDir(file);
//						System.out.println("delete fileName : " + fileName + " is " + delSucess);
						continue;
					}
					tempFileName = fileName;
				}
			}
		}
		
		File oldfile = new File(oldFilePath + "/" + tempFileName);
		File newfile = new File(newFilePath);

		if (oldfile.renameTo(newfile)) {
			logger.info("renamed file " + oldFilePath + " to " + newFilePath);
		} else {
			logger.info("the file " + oldFilePath + " can't be renamed");
		}
	}
}
