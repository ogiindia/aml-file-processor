package com.aml.file.pro.core.efrmsrv.startup.config;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

public class ColumnMapping implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SerializedName("file_field")
    private String fileField;

    @SerializedName("csv")
    private String csv;

    @SerializedName("parquet")
    private String parquet;

    @SerializedName("tbl_column")
    private String tblColumn;

    @SerializedName("db")
    private String db;

    @SerializedName("type")
    private String type;

    @SerializedName("pattern")
    private String pattern;

    @SerializedName("transform")
    private String transform;

    @SerializedName("nullable")
    private Boolean nullable;

    @SerializedName("file_field")
	public String getFileField() {
		return fileField;
	}

    @SerializedName("file_field")
	public void setFileField(String fileField) {
		this.fileField = fileField;
	}

    @SerializedName("csv")
	public String getCsv() {
		return csv;
	}

    @SerializedName("csv")
	public void setCsv(String csv) {
		this.csv = csv;
	}

    @SerializedName("parquet")
	public String getParquet() {
		return parquet;
	}

    @SerializedName("parquet")
	public void setParquet(String parquet) {
		this.parquet = parquet;
	}

    @SerializedName("tblColumn")
	public String getTblColumn() {
		return tblColumn;
	}

    @SerializedName("tblColumn")
	public void setTblColumn(String tblColumn) {
		this.tblColumn = tblColumn;
	}

    @SerializedName("db")
	public String getDb() {
		return db;
	}

    @SerializedName("db")
	public void setDb(String db) {
		this.db = db;
	}

    @SerializedName("type")
	public String getType() {
		return type;
	}

    @SerializedName("type")
	public void setType(String type) {
		this.type = type;
	}

    @SerializedName("pattern")
	public String getPattern() {
		return pattern;
	}

    @SerializedName("pattern")
	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

    @SerializedName("transform")
	public String getTransform() {
		return transform;
	}

    @SerializedName("transform")
	public void setTransform(String transform) {
		this.transform = transform;
	}

    @SerializedName("nullable")
	public Boolean getNullable() {
		return nullable;
	}

    @SerializedName("nullable")
	public void setNullable(Boolean nullable) {
		this.nullable = nullable;
	}

}
