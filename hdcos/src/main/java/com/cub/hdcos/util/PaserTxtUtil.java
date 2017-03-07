package com.cub.hdcos.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
//com.cub.hdcos.util.PaserTxtUtil
public class PaserTxtUtil {
	
	//public static String filePath = "/Volumes/Transcend/git/hdcos/file/cubinfotypea.txt";
	
	public static void main(String[] args) {
		String filePath = args[0];
		File file = new File(filePath);
		if(file.exists()) {
			FileReader fr = null;
			BufferedReader bfr = null;
			try {
				fr = new FileReader(file);
				bfr = new BufferedReader(fr);
				
				while(bfr.ready()) {
					String line = bfr.readLine();
					//System.out.println(line);
					String id = line.substring(0 , 5);//5
					String name = line.substring(5, 15);//10
					String tel = line.substring(15 , 26);//11
					String addr = line.substring(26, 36);//10
					String team = line.substring(36, 46);//10
					System.out.print("id : " + id);
					System.out.print("name : " + name);
					System.out.print("tel : " + tel);
					System.out.print("addr : " + addr);
					System.out.print("team : " + team);
					System.out.println();
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					bfr.close();
					fr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
