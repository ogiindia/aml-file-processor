package com.aml.file.pro.core.efrmsrv.filewatcher.service;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.file.pro.core.efrmsrv.utils.DateFormatUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

@Component
public class ParquetFileConverterService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileConverterService.class);
	
	@Autowired
	DateFormatUtils dateFormatUtils;
	
	/**
	 * 
	 * @param csvPath
	 * @param parquetPath
	 * @throws IOException
	 * @throws CsvException
	 */
	 public void convertCsvToParquet(String csvPath, String parquetPath) throws IOException, CsvException {
	        try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
	            // 1) Read header row (dynamic column names)
	            String[] headers = reader.readNext();
	            String[] firstRow = reader.readNext();     // first data row
	            if (headers == null) {
	                throw new IllegalArgumentException("Empty CSV file, no header row found");
	            }

	            int colCount = headers.length;
	            String[] columnTypes = new String[colCount];
	            for (int i = 0; i < colCount; i++) {
	                String value = (firstRow != null && i < firstRow.length) ? firstRow[i] : null;
	                String colName = headers[i].trim().replaceAll("\\s+", "_");
	                columnTypes[i] = inferTypeFromValue(value,colName);  // STRING / INT / LONG / DOUBLE / BOOLEAN
	            }
	            
	            // 2) Build dynamic Parquet schema from headers
	            //    Here we make every column: optional binary <name> (UTF8)
	           // StringBuilder sb = new StringBuilder("message dynamic_schema {\n");
	           
	            String schemaString = buildSchemaString(headers, columnTypes);
	            LOGGER.info("Parquet schema: {}",schemaString);
	            MessageType schema = MessageTypeParser.parseMessageType(schemaString);
	            SimpleGroupFactory factory = new SimpleGroupFactory(schema);

	         // After you have built the schema:
	            LOGGER.info("---- EFFECTIVE PARQUET SCHEMA ----");
	            LOGGER.info(schema.toString());      // if you have org.apache.parquet.schema.MessageType
	            // or, if you use a string:
	            LOGGER.info(schemaString);
	            LOGGER.info("---- END SCHEMA ----");
	            
	            java.nio.file.Path localPath = Paths.get(parquetPath+"/output_"+new Date().getTime()+".parquet");
            	org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(localPath.toUri());
            	Configuration conf = new Configuration();
	            
	            // 3) Create Parquet writer
				try (ParquetWriter<Group> writer = ExampleParquetWriter
						.builder(HadoopOutputFile.fromPath(hadoopPath, conf))
						.withConf(conf)
						.withType(schema).build()) {
	            	
					 // write firstRow if present
	                if (firstRow != null) {
	                    Group g = factory.newGroup();
	                    writeRow(g, headers, columnTypes, firstRow);
	                    writer.write(g);
	                }
					
					/*
					 * Group g = factory.newGroup(); writeRow(g, headers, columnTypes, firstRow);
					 * writer.write(g);
					 */

	            	// then remaining rows
	            	String[] row;
	            	while ((row = reader.readNext()) != null) {
	            	    Group grp = factory.newGroup();
	            	    writeRow(grp, headers, columnTypes, row);
	            	    writer.write(grp);
	            	}
	            	writer.close(); 
	   
	            }

	            LOGGER.info("Wrote parquet file: {}", parquetPath);
	            LOGGER.info("Columns: {}", Arrays.toString(headers));
	        }
	    }

	    private String inferTypeFromValue(String value, String colName) {
	    	try {
	        if (value == null || value.isEmpty()) return "STRING";

	        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))
	            return "BOOLEAN";
	        
	        String lowerName = colName.toLowerCase();
	        String trimmed = value.trim();
	     // 1) Geometry (simple WKT detection)
	        if (lowerName.contains("geom") || lowerName.contains("wkt")
	            || trimmed.startsWith("POINT(")
	            || trimmed.startsWith("LINESTRING(")
	            || trimmed.startsWith("POLYGON(")) {
	            return "GEOMETRY";
	        }
	        try { Integer.parseInt(value); return "INT"; } catch (NumberFormatException ignored) {}
	        try { Long.parseLong(value);    return "LONG"; } catch (NumberFormatException ignored) {}
	        try { Double.parseDouble(value);return "DOUBLE"; } catch (NumberFormatException ignored) {}
	        try {  BigDecimal bd = new BigDecimal(value).setScale(2);  long unscaled = bd.unscaledValue().longValueExact();return "DECIMAL"; } catch (Exception ignored) {}
	        try {  if(dateFormatUtils.isDate(value)) {return "TIMESTAMP";}  } catch (Exception ignored) {}
	        
	    	} catch (Exception e) {
	    		LOGGER.error("Exception found in inferTypeFromValue : {}",e);
	    	} finally {
	    		
	    	}
	    	return "STRING";
	    }  
	    
	    // -------- schema builder --------
	    private static String buildSchemaString(String[] headers, String[] columnTypes) {
	    	
	        StringBuilder sb = new StringBuilder("message dynamic_schema {\n");

	        for (int i = 0; i < headers.length; i++) {
	            String colName = headers[i].trim().replaceAll("\\s+", "_");
	            String type = columnTypes[i];

	            switch (type) {
	                case "INT":
	                    sb.append("  optional int32 ").append(colName).append(";\n");
	                    break;
	                case "LONG":
	                    sb.append("  optional int64 ").append(colName).append(";\n");
	                    break;
	                case "DOUBLE":
	                    sb.append("  optional double ").append(colName).append(";\n");
	                    break;
	                case "BOOLEAN":
	                    sb.append("  optional boolean ").append(colName).append(";\n");
	                    break;
	                case "TIMESTAMP":
	                    sb.append("  optional int64 ")
	                      .append(colName)
	                      .append(" (TIMESTAMP(MILLIS,true));\n");
	                    break;
	                case "DECIMAL":
	                    sb.append("  optional int64 ")
	                      .append(colName)
	                      .append(" (DECIMAL(18,2));\n");
	                    break;
	                case "GEOMETRY":
	                    // IMPORTANT: store as plain UTF8 text, no GEOMETRY logical type
	                    sb.append("  optional binary ")
	                      .append(colName)
	                      .append(" (UTF8);\n");
	                    break;
	                default:
	                    sb.append("  optional binary ")
	                      .append(colName)
	                      .append(" (UTF8);\n");
	                    break;
	            }
	        }
	        sb.append("}\n");
	        return sb.toString();
	    }
	    
	    private void writeRow(Group g, String[] headers, String[] columnTypes, String[] row) {
	        for (int i = 0; i < headers.length; i++) {
	            String colName = headers[i].trim().replaceAll("\\s+", "_");
	            String type = columnTypes[i];
	            String value = (i < row.length) ? row[i] : null;

	            if (value == null || value.isEmpty()) continue;

	            switch (type) {
	                case "INT":
	                    g.append(colName, Integer.parseInt(value));
	                    break;
	                case "LONG":
	                    g.append(colName, Long.parseLong(value));
	                    break;
	                case "DOUBLE":
	                    g.append(colName, Double.parseDouble(value));
	                    break;
	                case "BOOLEAN":
	                    g.append(colName, Boolean.parseBoolean(value));
	                    break;
	                case "DECIMAL": {
	                    BigDecimal bd = new BigDecimal(value).setScale(2);
	                    long unscaled = bd.unscaledValue().longValueExact();
	                    g.append(colName, unscaled);
	                    break;
	                }
	                case "TIMESTAMP":
	                   // long epochMillis = parseToEpochMillis(value); // your parser
	                	long epochMillis =  getTimeStamp(value);
	                    g.append(colName, epochMillis);
	                    break;
	                case "GEOMETRY": {
	                    // value is a WKT string from CSV, just store as text
	                    g.append(colName, value);     // because schema uses binary (UTF8)
	                    break;
	                }
	                default:
	                    g.append(colName, value);
	                    break;
	            }
	        }
	    }
	    
	    private  long parseToEpochMillis(String value) {
	        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	        Timestamp tmp=  dateFormatUtils.toTimestamp(value);
	        
	        LocalDateTime ldt = LocalDateTime.parse(value, fmt);
	        return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();  // or ZoneId.systemDefault()
	    }
	    
	    private long getTimeStamp(String value) {
	      
	        Timestamp tmp=  dateFormatUtils.toTimestamp(value);
	    
	        return tmp.getTime();  // or ZoneId.systemDefault()
	    }
	   
}
