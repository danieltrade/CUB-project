package com.cub.hdcos.postgres.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cub.hdcos.exception.CUBErrorCode;
import com.cub.hdcos.exception.CUBException;
import com.cub.hdcos.postgres.entity.ColumninfoList;
import com.cub.hdcos.postgres.entity.MetaDataList;
import com.cub.hdcos.postgres.jdbc.PostgresDbDao;

public class MetaInfoService extends PostgresDbDao {
	
	final static Logger logger = Logger.getLogger(MetaInfoService.class);
	
	public static MetaInfoService getInstance(String db) {
		return new MetaInfoService(db);
	}
	
	private MetaInfoService(String db) {
		super(db);
	}
	
	public MetaDataList queryMetaDataList(String tableName) throws CUBException {
		MetaDataList metaDataInfo = new MetaDataList();
		StringBuffer sql = new StringBuffer();
		sql.append("select tableName , fileName , fileFormat , delimiter , fixformat_maxlength , filecode from meta_data_list where tableName = ? ");
		try {
			setParams(tableName);
			executeQuery(sql.toString());
			while (rs.next()) {
				metaDataInfo.setTableName(rs.getString("tableName"));
				metaDataInfo.setFileName(rs.getString("fileName"));
				metaDataInfo.setFileFormat(rs.getString("fileFormat"));//0 : delimited ; 1 : fixformat ; 2 : both
				metaDataInfo.setDelmiter(rs.getString("delimiter"));
				metaDataInfo.setFixformatMaxlength(rs.getString("fixformat_maxlength"));
				metaDataInfo.setFileCode(rs.getString("filecode"));
			}
			metaDataInfo.setColumns(queryColumns(tableName));
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_METADATALIST_001 , MetaInfoService.class.getName());
		}
		return metaDataInfo;
	}
	
	public List<ColumninfoList> queryColumns(String tableName) throws CUBException {
		List<ColumninfoList> columns = new ArrayList<ColumninfoList>();
		StringBuffer sql = new StringBuffer();
		sql.append("select                         ");
		sql.append(" id                            ");
		sql.append(",filename                      ");
		sql.append(",trim(column_name) column_name ");
		sql.append(",is_unique_key                 ");
		sql.append(",column_length                 ");
		sql.append(",column_fill_type              ");
		sql.append(",column_fill_char              ");
		sql.append(",column_type                   ");
		sql.append(",column_desc                   ");
		sql.append(",column_sensitivity_code       ");
		sql.append(",chartype                 ");
		sql.append("from columninfo_list           ");
		sql.append("where tableName = ?            ");
		try {
			setParams(tableName);
			executeQuery(sql.toString());
			while (rs.next()) {
				ColumninfoList column = new ColumninfoList();
				column.setId(rs.getString("id"));
				column.setFileName(rs.getString("filename"));
				column.setColumnName(rs.getString("column_name"));
				column.setIsUniqueKey(rs.getString("is_unique_key"));
				column.setColumnLength(rs.getString("column_length"));
				column.setColumnFillType(rs.getString("column_fill_type"));
				column.setColumnFillChar(rs.getString("column_fill_char"));
				column.setColumnType(rs.getString("column_type"));
				column.setColumnDesc(rs.getString("column_desc"));
				column.setColumnSensitivityCode(rs.getString("column_sensitivity_code"));
				column.setColumnCharType(rs.getString("chartype"));
				columns.add(column);
			}
		} catch (SQLException e) {
			logger.error(e.getMessage() , e);
			throw new CUBException(e.getMessage(), CUBErrorCode.CUB_POSTGRES_COLUMNINFOLIST_001 , MetaInfoService.class.getName());
		}
		return columns;
	}
}
