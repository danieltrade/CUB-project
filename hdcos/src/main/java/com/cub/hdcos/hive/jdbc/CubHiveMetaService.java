package com.cub.hdcos.hive.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.hive.entity.HiveColumnBean;
import com.cub.hdcos.hive.entity.HiveQLBean;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.util.StringUtil;


public class CubHiveMetaService extends HiveDbDao {
	
	final static Logger logger = Logger.getLogger(CubHiveMetaService.class);
	
	public static CubHiveMetaService getInstance(String db) {
		return new CubHiveMetaService(db);
	}
	
	public CubHiveMetaService(String db) {
		super(db);
	}
	
	public void createDatabase(String databaseName) throws CUBException {
		try {
			String sql = "create database if not exists " + databaseName;
			executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_001 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	
	/**
	 * create hive table by row format DELIMITED
	 * @param hqlBean HiveQLBean
	 * @throws CUBException
	 * */
	public void createTableByRowFormatDelimited(HiveQLBean hqlBean) throws CUBException {
		StringBuilder sql = new StringBuilder();
		sql.append(" create " + hqlBean.getTableType() + " table if not exists " + hqlBean.getTableName() + " ( ");
		List<HiveColumnBean> columns = hqlBean.getColumns();
		for(int i = 0 ; i < columns.size() ; i++) {
			HiveColumnBean column = columns.get(i);
			if(i != 0) {
				sql.append(",");
			}
			sql.append(" " + column.getColName() + " " + column.getDataType() + " COMMENT '" + column.getComment() + "' ");
		}
		sql.append(" )  ");
		sql.append(" row format DELIMITED ");
		if(StringUtil.isNotBlank(hqlBean.getFieldsSplit())) {
			sql.append(" fields terminated by '" + hqlBean.getFieldsSplit() + "' ");
		}
		if(StringUtil.isNotBlank(hqlBean.getCollectionItemsSplit())) {
			sql.append(" collection items terminated by '" + hqlBean.getCollectionItemsSplit() + "' ");
		}
		if(StringUtil.isNotBlank(hqlBean.getMapKeysSplit())) {
			sql.append(" map keys terminated by '" + hqlBean.getMapKeysSplit() + "' ");
		}
		if(StringUtil.isNotBlank(hqlBean.getLinesSplit())) {
			sql.append(" lines terminated by '" + hqlBean.getLinesSplit() + "' ");
		}
		if(StringUtil.isNotBlank(hqlBean.getFileFormat())) {
			sql.append(" stored as " + hqlBean.getFileFormat() + " ");
		}
		if(StringUtil.isNotBlank(hqlBean.getHdfsPath())) {
			sql.append(" location " + hqlBean.getHdfsPath());
		}
		try {
			executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_002 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	/**
	 * create hive table by row format SERDE
	 * @param hqlBean HiveQLBean
	 * @throws CUBException
	 * */
	public void createTableByRowFormatSerde(HiveQLBean hqlBean) throws CUBException {
		StringBuilder sql = new StringBuilder();
		sql.append(" create " + hqlBean.getTableType() + " table if not exists " + hqlBean.getTableName() + " ( ");
		List<HiveColumnBean> columns = hqlBean.getColumns();
		for(int i = 0 ; i < columns.size() ; i++) {
			HiveColumnBean column = columns.get(i);
			if(i != 0) {
				sql.append(",");
			}
			sql.append(" " + column.getColName() + " " + column.getDataType() + " COMMENT '" + column.getComment() + "' ");
		}
		sql.append(" )  ");
		sql.append(" row format SERDE " + hqlBean.getSerdeClass());
		//sb.append(" location " + hqlBean.getHdfsPath());
		Map<String,String> serdeProperties = hqlBean.getSerdeProperties();
		if(serdeProperties != null && !serdeProperties.isEmpty()) {
			Set<String> keys = serdeProperties.keySet();
			sql.append(" with serdeproperties ( ");
			//'input.regex' = 'bduid\\[(.*)\\]uid\\[(\\d+)\\]uname\\[(.*)\\]', 
			int i = 0;
			for(String key : keys) {
				if(i != 0) {
					sql.append(",");
				}
				sql.append(" '" + key + "' = '" + serdeProperties.get(key) + "'");
				i++;
			}
			sql.append(" ) ");
		}
		if(hqlBean.getFileFormat()!= null && !"".equals(hqlBean.getFileFormat())) {
			sql.append(" STORED AS " + hqlBean.getFileFormat());
		} else if(hqlBean.getInputFormatClass() != null && !"".equals(hqlBean.getInputFormatClass())) {
			sql.append(" STORED AS INPUTFORMAT '" + hqlBean.getInputFormatClass() + "'");
			sql.append(" OUTPUTFORMAT '" + hqlBean.getOutputFormatClass() + "'");
		}
		
		if(hqlBean.getHdfsPath() != null && !"".equals(hqlBean.getHdfsPath())) {
			sql.append(" location '" + hqlBean.getHdfsPath() + "' ");
		}
		if(hqlBean.getSourceTable() != null && !"".equals(hqlBean.getSourceTable())) {
			sql.append(" as select * from " + hqlBean.getSourceTable());
		}
		try {
			executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_002 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	/**
	 * drop hive table
	 * @param tableName String
	 * @throws CUBException 
	 * */
	public void dropTable(String tableName) throws CUBException {
		try {
			String sql = "drop table if exists " + tableName;
			executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_003 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	/**
	 * drop hive view
	 * @param viewName String
	 * @throws CUBException 
	 * */
	public void dropView(String viewName) throws CUBException {
		try {
			String sql = "drop view " + viewName;
			executeUpdate(sql);
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_011 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	
	/**
	 * insert select table
	 * @param insertTable String (insert table name)
	 * @param sourceTable String (select table name)
	 * @throws CUBException 
	 * */
	public void insertSelectTable(String insertTable , String sourceTable) throws CUBException {
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("insert overwrite table ");
			sql.append(insertTable);
			sql.append(" select * from ");
			sql.append(sourceTable);
			executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_004 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	/**
	 * load HDFS data into hive table
	 * @param filepath String (HDFS file path)
	 * @param tableName String (hive table name)
	 * @throws CUBException 
	 * */
	public void loadHDFSDataIntoTable(String filepath , String tableName) throws CUBException {
		loadDataInputTable(false, filepath, tableName);
	}
	/**
	 * load loacal data into hive table
	 * @param filepath String (local file path)
	 * @param tableName String (hive table name)
	 * @throws CUBException 
	 * */
	public void loadLocalDataIntoTable(String filepath , String tableName) throws CUBException {
		loadDataInputTable(true, filepath, tableName);
	}
	/**
	 * load hive table into HDFS file
	 * @param loadPath String (HDFS file path)
	 * @param tableName String (hive table name)
	 * @throws CUBException 
	 * */
	public void loadHiveTableIntoHDFS(String loadPath , String tableName) throws CUBException {
		loadLocalTableIntoFile(loadPath, ' ', tableName, false);
	}
	/**
	 * load hive table into HDFS file
	 * @param loadPath String (HDFS file path)
	 * @param splitChar char (檔案資料分割字元)
	 * @param tableName String (hive table name)
	 * @throws CUBException 
	 * */
	public void loadHiveTableIntoHDFS(String loadPath , char splitChar , String tableName) throws CUBException {
		loadLocalTableIntoFile(loadPath, splitChar, tableName, false);
	}
	/**
	 * load hive table into local file
	 * @param loadPath String (local file path)
	 * @param tableName String (hive table name)
	 * @throws CUBException 
	 * */
	public void loadHiveTableIntoLocalFile(String loadPath , String tableName) throws CUBException {
		loadLocalTableIntoFile(loadPath, ' ', tableName, false);
	}
	/**
	 * load hive table into local file
	 * @param loadPath String (local file path)
	 * @param splitChar char (檔案資料分割字元)
	 * @param tableName String (hive table name)
	 * @throws CUBException 
	 * */
	public void loadHiveTableIntoLocalFile(String loadPath , char splitChar , String tableName) throws CUBException {
		loadLocalTableIntoFile(loadPath, splitChar, tableName, false);
	}
	/**
	 * create hive view from table
	 * @param hqlBean HiveQLBean (viewName , columns , tableName)
	 * @throws CUBException
	 * */
	public void createView(HiveQLBean hqlBean) throws CUBException {
		try {
			String viewName = hqlBean.getViewName();
			StringBuilder sql = new StringBuilder();
			sql.append("create view " + viewName + " as select ");
			List<HiveColumnBean> columns = hqlBean.getColumns();
			for(int i = 0 ; i < columns.size() ; i++) {
				HiveColumnBean column = columns.get(i);
				if(i != 0) {
					sql.append(",");
				}
				sql.append(column.getColName());
			}
			sql.append(" from ");
			String tableName = hqlBean.getTableName();
			sql.append(tableName);
			executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_007 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	/**
	 * 取得當前資料庫下所有tables
	 * @return tablesName List<String>
	 * @throws CUBException 
	 * */
	public List<String> showTables() throws CUBException {
		List<String> tables = new ArrayList<String>();
		try {
			String sql = "show tables";
			executeQuery(sql);
			while(rs.next()) {
				String tableName = rs.getString(1);
				tables.add(tableName);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_008 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
		return tables;
	}
	/**
	 * 取得table欄位資訊
	 * @param tableName String
	 * @return table schema List<ColumninfoList>
	 * @throws CUBException 
	 * */
	public List<ColumninfoList> describeTable(String tableName) throws CUBException {
		List<ColumninfoList> schema = new ArrayList<ColumninfoList>();
		String sql = "describe " + tableName;
		System.out.println(sql);
		try {
			executeQuery(sql);
			while(rs.next()) {
				ColumninfoList column = new ColumninfoList();
				column.setColumnName(rs.getString(1));
				column.setColumnType(rs.getString(2));
				column.setColumnDesc(rs.getString(3));
				schema.add(column);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_009 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
		return schema;
	}
	/**
	 * 查詢table資料
	 * @param tableName String
	 * @param columns String[] (查詢欄位)
	 * @return dataList List<Map<String, Object>>
	 * @throws CUBException 
	 * */
	public List<Map<String, Object>> queryTable(String tableName , String[] columns) throws CUBException {
		List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
		try {
			StringBuffer sb = new StringBuffer();
			sb.append("SELECT ");
			for(int i = 0 ; i < columns.length ; i++) {
				if(i != 0) {
					sb.append(" , ");
				}
				String column = columns[i];
				sb.append(column);
			}
			sb.append(" from ");
			sb.append(tableName);
			
			executeQuery(sb.toString());
			while(rs.next()) {
				Map<String, Object> temp = new HashMap<String, Object>();
				for(String column : columns) {
					temp.put(column, rs.getString(column));
				}
				dataList.add(temp);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_010 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
		return dataList;
	}
	
	private void loadDataInputTable(boolean isLocal , String filepath , String tableName) throws CUBException {
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("load data ");
			if(isLocal) {
				sql.append("local ");
			}
			sql.append("inpath '");
			sql.append(filepath);
			sql.append("' into table ");
			sql.append(tableName);
			executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_005 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	
	private void loadLocalTableIntoFile(String loadPath , char splitChar , String tableName , boolean isLocal) throws CUBException {
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("insert overwrite ");
			if(isLocal) {
				sql.append("local ");
			}
			sql.append("directory '" + loadPath + "' ");
			if(!Character.isWhitespace(splitChar)) {
				sql.append("row format delimited fields terminated by '" + splitChar + "' ");
			}
			sql.append("select * from " + tableName);
			executeUpdate(sql.toString());
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_HIVE_006 , CubHiveMetaService.class.getName());
		} finally {
			close();
		}
	}
	
}
