package com.aml.file.pro.core.recordDTO;

import java.io.Serializable;

public class MapperSummarizationFiledImpl implements MapperSummarizationFiledDTO, Serializable {
	
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String fieldName;
    private String tableName;
    private String columnName;
    private String whereClauseCloumn;

    public MapperSummarizationFiledImpl() {}

    public MapperSummarizationFiledImpl(String fieldName, String tableName,String columnName, String whereClauseCloumn) {
        this.fieldName = fieldName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.whereClauseCloumn = whereClauseCloumn;
    }

    @Override
    public String getFieldName() { return fieldName; }
    public void setFieldName(String fieldName) { this.fieldName = fieldName; }

    @Override
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	@Override
	public String getWhereClauseCloumn() {
		return whereClauseCloumn;
	}

	public void setWhereClauseCloumn(String whereClauseCloumn) {
		this.whereClauseCloumn = whereClauseCloumn;
	}

   
}

