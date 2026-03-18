package com.aml.file.pro.core.efrmsrv.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class DateFormatUtils {

	// List of possible formats you expect
	private static final String[] DATE_PATTERNS = 
		{ "dd-MM-yyyy","dd-MMM-yyyy",
		  "dd-MMM-yy hh.mm.ss.nnnnnnnnn a",
		  "yyyy-MM-dd HH:mm:ss", 
		  "yyyy-MM-dd", 
		  "dd/MM/yyyy", 
	      "MM/dd/yyyy",
		  "dd-MM-yyyy HH:mm:ss", "MM-dd-yyy"};
	
	public Timestamp getTimestampValue(Map<String, Object> map, String key) {
        Object value = map.getOrDefault(key, null);
        if (value == null) {
            return null;
        }
        // Already a Timestamp
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        // Already a Date
        if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        }

        // Try parsing string with multiple patterns
        String strVal = value.toString().trim();
        for (String pattern : DATE_PATTERNS) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                Date parsed = sdf.parse(strVal);
                return new Timestamp(parsed.getTime());
            } catch (Exception ignored) {
                // try next pattern
            }
        }

        System.out.println("Could not parse date for key: " + key + " value: " + strVal);
        return null;
    }
	
	public java.sql.Timestamp toTimestamp(String str) {
	   // String[] patterns = {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy"};
	    for (String p : DATE_PATTERNS) {
	        try {
	            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(p);
	            return new java.sql.Timestamp(sdf.parse(str).getTime());
	        } catch (Exception ignored) {}
	    }
	    return null;
	}
	
	public boolean isDate(String str) {
	    //String[] patterns = {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "dd/MM/yyyy"};
	    for (String p : DATE_PATTERNS) {
	        try {
	            new java.text.SimpleDateFormat(p).parse(str);
	            return true;
	        } catch (Exception ignored) {}
	    }
	    return false;
	}
}
