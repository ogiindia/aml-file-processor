package com.aml.file.pro.core.efrmsrv.utils;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "file.nrt")
public class FileProcessProConfig {

	private String sourcepath;
	private String destinationpath;
	private String processedpath;
	private String fromfileformat;
	private String tofileformat;
	private boolean ismove;
	private boolean isdelete;
	private Integer filecount;
	private Integer fetchinterval;
	private Integer completioncheckinterval;
	private String txntablenameprefix;
	private Integer batchcount;
	private String dbtype;
	private String parqutefilepath;
	private String xmlroottag;
	private String startconfigpath;
	
	public String getSourcepath() {
		return sourcepath;
	}

	public void setSourcepath(String sourcepath) {
		this.sourcepath = sourcepath;
	}

	public String getDestinationpath() {
		return destinationpath;
	}

	public void setDestinationpath(String destinationpath) {
		this.destinationpath = destinationpath;
	}

	public String getProcessedpath() {
		return processedpath;
	}

	public void setProcessedpath(String processedpath) {
		this.processedpath = processedpath;
	}

	public String getFromfileformat() {
		return fromfileformat;
	}

	public void setFromfileformat(String fromfileformat) {
		this.fromfileformat = fromfileformat;
	}

	public String getTofileformat() {
		return tofileformat;
	}

	public void setTofileformat(String tofileformat) {
		this.tofileformat = tofileformat;
	}

	public boolean isIsmove() {
		return ismove;
	}

	public void setIsmove(boolean ismove) {
		this.ismove = ismove;
	}

	public boolean isIsdelete() {
		return isdelete;
	}

	public void setIsdelete(boolean isdelete) {
		this.isdelete = isdelete;
	}

	public Integer getFilecount() {
		return filecount;
	}

	public void setFilecount(Integer filecount) {
		this.filecount = filecount;
	}

	public Integer getFetchinterval() {
		return fetchinterval;
	}

	public void setFetchinterval(Integer fetchinterval) {
		this.fetchinterval = fetchinterval;
	}

	public Integer getCompletioncheckinterval() {
		return completioncheckinterval;
	}

	public void setCompletioncheckinterval(Integer completioncheckinterval) {
		this.completioncheckinterval = completioncheckinterval;
	}

	public String getTxntablenameprefix() {
		return txntablenameprefix;
	}

	public void setTxntablenameprefix(String txntablenameprefix) {
		this.txntablenameprefix = txntablenameprefix;
	}

	public Integer getBatchcount() {
		return batchcount;
	}

	public void setBatchcount(Integer batchcount) {
		this.batchcount = batchcount;
	}

	public String getDbtype() {
		return dbtype;
	}

	public void setDbtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getParqutefilepath() {
		return parqutefilepath;
	}

	public void setParqutefilepath(String parqutefilepath) {
		this.parqutefilepath = parqutefilepath;
	}

	public String getXmlroottag() {
		return xmlroottag;
	}

	public void setXmlroottag(String xmlroottag) {
		this.xmlroottag = xmlroottag;
	}

	public String getStartconfigpath() {
		return startconfigpath;
	}

	public void setStartconfigpath(String startconfigpath) {
		this.startconfigpath = startconfigpath;
	}
	
}