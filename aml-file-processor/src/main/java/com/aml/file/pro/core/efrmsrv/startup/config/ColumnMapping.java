package com.aml.file.pro.core.efrmsrv.startup.config;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnMapping implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SerializedName("from")
    private String from;

	@SerializedName("to")
    private String to;
   
    @SerializedName("type")
    private String type;

    @SerializedName("from")
	public String getFrom() {
		return from;
	}

    @SerializedName("from")
	public void setFrom(String from) {
		this.from = from;
	}

    @SerializedName("to")
	public String getTo() {
		return to;
	}

    @SerializedName("to")
	public void setTo(String to) {
		this.to = to;
	}

    @SerializedName("type")
	public String getType() {
		return type;
	}

    @SerializedName("type")
	public void setType(String type) {
		this.type = type;
	}
}