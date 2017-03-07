package com.cub.hdcos.shell.execute;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.cub.hdcos.util.StringUtil;

public class TeradataEncodeTransShell {

	public static void main(String[] args) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String teradataDatabaseName = "DP_MCIF_OVERSEA";
			String teradataTableName = "PARTY_CNC_TEST";
			StringBuffer tableColumns = new StringBuffer();
			StringBuffer charTypes = new StringBuffer();
			StringBuffer selectcolumns = new StringBuffer();
			
			String connurl = "jdbc:teradata://88.8.71.221/DP_MCIF_OVERSEA,tmode=ANSI,charset=UTF8";
			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(connurl, "nt12533", "moss1230");
			
			String sql = "select * from  DBC.Columns where databasename = '" + teradataDatabaseName + "' and TABLENAME='" + teradataTableName + "'";
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			for(int i = 0 ; rs.next() ; i++) {
				String databaseName = rs.getString("databaseName");
				String tableName = rs.getString("tableName");
				String columnName = rs.getString("columnName");
				String columnType = rs.getString("columnType");
				String charType = rs.getString("charType");
//				System.out.println("columnName : " + columnName + " , charType : " + charType);
				if(i != 0) {
					tableColumns.append(",");
					charTypes.append(",");
					selectcolumns.append(",");
				}
				tableColumns.append(columnName);//CHAR2HEXINT
				charTypes.append(charType);
				selectcolumns.append("CHAR2HEXINT(" + columnName + ")");
			}
			
			String[] columns = trimArraydata(tableColumns.toString().split(","));
			String[] chartypes = trimArraydata(charTypes.toString().split(","));
			
			Map<String,String> chartypeMapping = new HashMap<String,String>();
			for(int i = 0 ; i < columns.length ; i++ ) {
				chartypeMapping.put(columns[i], chartypes[i]);
			}
			rs = null;
			List<Map<String,String>> datas = new ArrayList<Map<String,String>>();
			sql = "select " + selectcolumns.toString() + " from " + teradataDatabaseName + "." + teradataTableName;
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			
			for(int i = 0 ; rs.next() ; i++) {
				Map<String,String> rowdata = new HashMap<String,String>();
				for(String column : columns) {
					String columnValue = StringUtil.parseNull(rs.getString(column)).trim();
					String charType = chartypeMapping.get(column);
					byte[] bytes = Hex.decodeHex(columnValue.toCharArray());
					if("2".equals(charType)) {
						columnValue = new String(bytes, "utf-8");
					} else {
						columnValue = new String(bytes, "big5");
					}
					
					rowdata.put(column, columnValue);
				}
				datas.add(rowdata);
			}
			
			showDatas(datas, columns);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (DecoderException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(stmt != null) {
					stmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if(conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private static String[] trimArraydata(String[] datas) {
		String[] retDatas = new String[datas.length];
		for(int i = 0 ; i < datas.length ; i++) {
			retDatas[i] = datas[i].trim();
		}
		return retDatas;
	}
	
	private static void showDatas(List<Map<String,String>> datas , String[] columns) {
		for(int i = 0 ; i < datas.size() ; i++) {
			Map<String,String> rowdata = datas.get(i);
			for(String column : columns) {
				String columnVal = rowdata.get(column);
				System.out.print(column + " : " + columnVal);
				System.out.print("    ");
			}
			System.out.print("\n");
		}
	}
}
