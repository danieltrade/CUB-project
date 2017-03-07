package com.cub.hdcos.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.Postzmsg;

public class PostzmsgLogger {
	
	final static Logger logger = Logger.getLogger(PostzmsgLogger.class);
	
	private static boolean usePostzmsg;
	private static String usersEmail;
	
	public static PostzmsgLogger getLogger() {
		return new PostzmsgLogger();
	}
	
	public PostzmsgLogger() {
		Properties prop = PropertiesUtil.getProperties(PropertiesUtil.HDCOS_CONFIG_PATH);
		this.usePostzmsg = ((prop.getProperty("use.postzmsg") == null) ? false : Boolean.valueOf(prop.getProperty("use.postzmsg").trim()));
		String usersemail = (prop.getProperty("user.postzmsg.email") == null ? "" : prop.getProperty("user.postzmsg.email"));
		this.usersEmail = usersemail;
	}
	
	public static String info(String message) {
		Postzmsg postzmsg = new Postzmsg();
		postzmsg.setMtype(message);
		return sendPostzmsg(postzmsg);
	}
	
	public static String info(Postzmsg message) {
		return sendPostzmsg(message);
	}
	
	public static String error(Postzmsg message) {
		return sendPostzmsg(message);
	}
	
	private static String sendPostzmsg(Postzmsg message) {
		StringBuffer command = new StringBuffer();
		command.append("postzmsg ");
		command.append("-r \"" + message.getRtype() + "\" ");
		command.append("-m \"" + message.getMtype() + "\" ");
		command.append("hostname=\"" + message.getHostName() + "\" ");//當前機器名字
		command.append("date=\"" + getSysDate() + "\" ");
		command.append("sub_source=\"CUB\" ");//塞CUB
		command.append("origin=\"" + message.getOrigin() + "\" ");//程式名稱
		command.append("sub_origin=\"" + message.getSubOrigin() + "\" ");//XXX(錯誤碼)
		command.append("adapter_host=\"" + message.getAdapterHost() + "\" ");//錯誤碼[XXX]
		command.append("msg_catalog=\"" + usersEmail + "\" ");//謀福email
		command.append("CUB_AP_NOTIFY CUB_AP");
		logger.info("Postzmsg command => " + command.toString());
		if(usePostzmsg) {
			return executeCommand(command.toString());
		} else {
			return "no execute sendPostzmsg...";
		}
	}
	
	private static String sendCustomPostzmsg(Postzmsg message) {
		StringBuffer command = new StringBuffer();
		command.append("postzmsg ");
		command.append("-r " + message.getRtype() + " ");
		command.append("-m \"" + message.getMtype() + "\" ");
		command.append("hostname=\"" + message.getHostName() + "\" ");
		command.append("date=\"" + message.getDate() + "\" ");
		command.append("sub_source=\"" + message.getSubSource() + "\" ");//塞CUB
		command.append("origin=\"" + message.getOrigin() + "\" ");
		command.append("sub_origin=\"" + message.getSubOrigin() + "\" ");
		command.append("adapter_host=\"" + message.getAdapterHost() + "\" ");
		command.append("msg_catalog=\"" + usersEmail + "\" ");//謀福email
		command.append("CUB_AP_NOTIFY CUB_AP");
		logger.info("Postzmsg command => " + command.toString());
		if(usePostzmsg) {
			return executeCommand(command.toString());
		} else {
			return "no execute sendPostzmsg...";
		}
	}
	
	private static String executeCommand(String command) {
		StringBuffer output = new StringBuffer();
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return output.toString();
	}
	
	public static String getSysDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssS");
		return sdf.format(new Date());
	}
	
	public static String getHostName() {
		String hostname = "";
		try {
//			InetAddress addr = InetAddress.getLocalHost();
//			hostname = addr.getHostName();
			
			InetAddress inetAddr = InetAddress.getLocalHost();
			byte[] addr = inetAddr.getAddress();

			// Convert to dot representation
			String ipAddr = "";
			for (int i = 0; i < addr.length; i++) {
				if (i > 0) {
					ipAddr += ".";
				}
				ipAddr += addr[i] & 0xFF;
			}
			hostname = inetAddr.getHostName();
			System.out.println("IP Address: " + ipAddr);
			System.out.println("Hostname: " + hostname);

		} catch (UnknownHostException ex) {
			System.out.println("Hostname can not be resolved");
		}
		return hostname;
	}
}
