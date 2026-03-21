package com.aml.file.pro.core.efrmsrv.filewatcher.service;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.file.pro.core.efrmsrv.startup.config.JsonConfigLoader;
import com.aml.file.pro.core.efrmsrv.startup.config.TransactionMapping;
import com.aml.file.pro.core.efrmsrv.utils.DateFormatUtils;
import com.aml.file.pro.core.recordDTO.TransactionCustomFieldRDTO;
import com.google.gson.Gson;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ParquetFileConverterService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileConverterService.class);
	
	@Autowired
	DateFormatUtils dateFormatUtils;
	
	@Autowired
	JsonConfigLoader jsonConfigLoader;
	
	/**
	 * 
	 * @param csvPath
	 * @param parquetPath
	 * @throws IOException
	 * @throws CsvException
	 */
	 public void convertCsvToParquet(String csvPath, String parquetPath) throws IOException, CsvException {
		 String schemaString = null;MessageType schema = null;
		  SimpleGroupFactory factory = null;
		  Configuration conf = null;
		  
		 try (CSVReader reader = new CSVReader(new FileReader(csvPath))) {
	            // 1) Read header row (dynamic column names)
	            String[] headers = reader.readNext();
	            String[] firstRow = reader.readNext();// first data row
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
	            
	            // 2) Build dynamic Parquet schema from headers - Here we make every column: optional binary <name> (UTF8)	          
	            schemaString = buildSchemaString(headers, columnTypes);
	            //LOGGER.info("Parquet schema: {}",schemaString);
	            schema = MessageTypeParser.parseMessageType(schemaString);
	            factory = new SimpleGroupFactory(schema);

	            // After you have built the schema:
	            LOGGER.debug("---- EFFECTIVE PARQUET SCHEMA ----");
	            LOGGER.debug(schema.toString());      // if you have org.apache.parquet.schema.MessageType or, if you use a string:
	            LOGGER.debug(schemaString);
	            LOGGER.debug("---- END SCHEMA ----");
	            
	            Path pth = Path.of(csvPath);
	            LOGGER.info("Source File Name : {}",pth.getFileName().toString());
	            String parquteName = getParquteFilNameWithPath(pth.getFileName().toString());
	            LOGGER.info("parquteName File Name and Path : {}",parquteName);
	            //java.nio.file.Path localPath = Paths.get(parquetPath+"/output_"+new Date().getTime()+".parquet");
	            java.nio.file.Path localPath = Paths.get(parquteName);
            	org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(localPath.toUri());
            	conf = new Configuration();
            	conf.setBoolean("dfs.client.write.checksum", false);
            	conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", true);
            	conf.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
	            // 3) Create Parquet writer
				try (ParquetWriter<Group> writer = ExampleParquetWriter
						.builder(HadoopOutputFile.fromPath(hadoopPath, conf))
						.withConf(conf)
						.withWriteMode(Mode.OVERWRITE)
						.withType(schema).build()) {
	            	
					 // write firstRow if present
	                if (firstRow != null) {
	                    Group g = factory.newGroup();
	                    writeRow(g, headers, columnTypes, firstRow);
	                    writer.write(g);
	                }
					
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
	        } catch(Exception e) {
	        	LOGGER.error("Exception found in ParquetFileConverterService@convertCsvToParquet : {}",e);
	        } finally {
	        	schemaString = null; schema = null; factory = null; conf = null;
	        }
	 }
	 
	 public void convertXlsXlsxToParqute(Path xlsxPathParam, String parquetPath) {
		 LOGGER.info("convertXlsXlsxToParqute Method Called......");
		 Workbook workbook = null; Sheet sheet = null; Row headerRow = null;
		 String xlsxFileName =null; Path destinationDir = null;
		 String schemaString = null;MessageType schema = null;
		  SimpleGroupFactory factory = null;
		  Configuration conf = null;
		  String praquteFileName = null;
		 try {
				destinationDir = Paths.get(parquetPath);
				if (!Files.exists(destinationDir)) {
					Files.createDirectories(destinationDir);
					LOGGER.info("Created destination folder: [{}]", destinationDir);
				}
				if (xlsxPathParam != null && Files.exists(xlsxPathParam) && Files.isRegularFile(xlsxPathParam)) {
					xlsxFileName = xlsxPathParam.getFileName().toString();
					LOGGER.info("Elcel XLSX file Name is : [{}]", xlsxFileName);
					praquteFileName = xlsxFileName.substring(0, xlsxFileName.lastIndexOf("."));
				}
			 
				if (xlsxPathParam.toString().endsWith(".xlsx")) {
					LOGGER.info("[XLSX] File is detected......");
					workbook = new XSSFWorkbook(Files.newInputStream(xlsxPathParam));
				} else if (xlsxPathParam.toString().endsWith(".xls")) {
					LOGGER.info("[XLS] File is detected......");
					workbook = new HSSFWorkbook(Files.newInputStream(xlsxPathParam));
				} else {
					// throw new IllegalArgumentException("Unsupported file type");
				}
				
				if (workbook != null) {
					// Read header row
					sheet = workbook.getSheetAt(0);
					headerRow = sheet.getRow(0);
										
					List<String> headerList = new ArrayList<>();
					for (Cell cell : headerRow) {
						headerList.add(cell.getStringCellValue().trim());
					}
					int colCount = headerList.size();
					String[] columnTypes = new String[colCount];
					Row row = sheet.getRow(0);
					 
					String[] firstRow = new String[colCount];
					//First Row 
					for (int i = 0; i < colCount; i++) {
						firstRow[i]=row.getCell(i).toString();
						String value = (row != null) ? row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL).toString() : null;
						String colName = headerList.get(i).trim().replaceAll("\\s+", "_");
						columnTypes[i] = inferTypeFromValue(value, colName); // STRING / INT / LONG / DOUBLE / BOOLEAN
						
					}
					String[] headerArray = headerList.toArray(new String[0]);
					// 2) Build dynamic Parquet schema from headers - Here we make every column: optional binary <name> (UTF8)
					schemaString = buildSchemaString(headerArray, columnTypes);
					//LOGGER.info("Parquet schema: {}", schemaString);
					schema = MessageTypeParser.parseMessageType(schemaString);
					factory = new SimpleGroupFactory(schema);
					 // After you have built the schema:
		            LOGGER.debug("---- EFFECTIVE PARQUET SCHEMA ----");
		            LOGGER.debug(schema.toString());      // if you have org.apache.parquet.schema.MessageType or, if you use a string:
		            LOGGER.debug(schemaString);
		            LOGGER.debug("---- END SCHEMA ----");
		            //FILENAME/CBS/year/month/date/file
		           
		            LOGGER.info("Source File Name ---> : {}",xlsxPathParam.getFileName().toString());
		            String parquteName = getParquteFilNameWithPath(xlsxPathParam.getFileName().toString());
		            LOGGER.info("parquteName File Name and Path ---> : {}",parquteName);
		            //java.nio.file.Path localPath = Paths.get(parquetPath+"/output_"+new Date().getTime()+".parquet");
		            java.nio.file.Path localPath = Paths.get(parquteName);
		            
		            //java.nio.file.Path localPath = Paths.get(parquetPath+"/"+praquteFileName+"_"+new Date().getTime()+".parquet");
		            LOGGER.info("Parqute File Name : [{}]",localPath);
		            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(localPath.toUri());
	            	conf = new Configuration(); 
	            	conf.setBoolean("dfs.client.write.checksum", false);
	            	conf.setBoolean("dfs.client.read.shortcircuit.skip.checksum", true);
	            	conf.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
	            	 // 3) Create Parquet writer
					try (ParquetWriter<Group> writer = ExampleParquetWriter
							.builder(HadoopOutputFile.fromPath(hadoopPath, conf))
							.withConf(conf)
							.withWriteMode(Mode.OVERWRITE)
							.withType(schema).build()) {
		            	
						// write firstRow if present
						if (firstRow != null) {
							Group g = factory.newGroup();
							writeRow(g, headerArray, columnTypes, firstRow);
							writer.write(g);
						}
		                
						for (int i = 1; i <= sheet.getLastRowNum(); i++) {
							Row rowRemain = sheet.getRow(i);
							String[] rowRm = new String[rowRemain.getLastCellNum()];
							for (int cell = 0; cell < rowRemain.getLastCellNum(); cell++) {
								Cell rowCell = rowRemain.getCell(cell);
							
								CellType celTyp = null;
								if(rowCell!=null) {
									celTyp = rowCell.getCellType();
								}
								String value="";
								if (rowCell != null && celTyp!=null) {
								switch (celTyp) {
								    case STRING:
								        value = rowCell.getStringCellValue();
								        break;
								    case NUMERIC:
								        value = String.valueOf(rowCell.getNumericCellValue());
								        if (DateUtil.isCellDateFormatted(rowCell)) {
								            // Convert to LocalDate or keep as Date
								            Date date = rowCell.getDateCellValue();
								            // Example: format as yyyy-MM-dd
								            value = new SimpleDateFormat("yyyy-MM-dd").format(date);
								        } else {
								            value = String.valueOf(rowCell.getNumericCellValue());
								        }
								        break;
								    case BOOLEAN:
								        value = String.valueOf(rowCell.getBooleanCellValue());
								        break;
									case FORMULA:
										value = rowCell.getCellFormula();
										break;
									case BLANK:
										value =  "";
										break;
									default:
								        value = "";
								}
								}
								rowRm[cell]=value;
							}

							if (rowRm != null) {
								Group grp = factory.newGroup();
								writeRow(grp, headerArray, columnTypes, rowRm);
								writer.write(grp);
							}
		                }
		            	writer.close(); 
		            }

					LOGGER.info("Wrote parquet file: {}", parquetPath);
					LOGGER.warn("Columns: {}", Arrays.toString(headerArray));
				}
			} catch (Exception e) {
				LOGGER.error("Exception found in ParquetFileConverterService@convertCsvToParquet : {}", e);
			} finally {
				schemaString = null; schema = null; factory = null; conf = null;
			}
	 }
	 	/**
	 	 * 
	 	 * @param value
	 	 * @param colName
	 	 * @return
	 	 */
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
	    	} finally { }
	    	return "STRING";
		}

	    // -------- schema builder --------
	    private String buildSchemaString(String[] headers, String[] columnTypes) {
	    	
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
	    
	    /**
	     * 
	     * @param g
	     * @param headers
	     * @param columnTypes
	     * @param row
	     */
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
	    /**
	     * 
	     * @param fileNameParam
	     * @return
	     */
		private String getParquteFilNameWithPath(String fileNameParam) {
			LOGGER.info("getParquteFilNameWithPath Method Called..........");
			String fileNameParqute = null;
			List<TransactionMapping> tranMppingLst = null;
			TransactionCustomFieldRDTO trancustFldDTOObj = null;
			try {
				tranMppingLst = jsonConfigLoader.getStartUpConfig();	
				if (tranMppingLst != null && tranMppingLst.size() > 0) {
					for (TransactionMapping tranMap : tranMppingLst) {
						String json = new Gson().toJson(tranMap);
						LOGGER.debug("----------->> {}",json);
						LOGGER.info("fileNameParam : [{}] - SourceFileName : [{}]",fileNameParam, tranMap.getSourceFileName());
						if (tranMap != null && StringUtils.isNotBlank(tranMap.getSourceFileName())
								&& StringUtils.isNotBlank(fileNameParam)
									&& fileNameParam.equalsIgnoreCase(tranMap.getSourceFileName())) {
								trancustFldDTOObj = new TransactionCustomFieldRDTO(tranMap.getDestFileType(),
										tranMap.getDestLocation(), tranMap.getSourceFileName(), tranMap.getSource(), tranMap.getShortName());
								fileNameParqute = toFormParquetPathUtils(trancustFldDTOObj);
								LOGGER.info("getParquteFilNameWithPath [IF]- fileNameParqute : {}", fileNameParqute);
							} else {
								LOGGER.warn("getParquteFilNameWithPath [ELSE]- fileNameParqute - JSON Not Configured : {}", fileNameParqute);
							}
						}
					}

			} catch (Exception e) {
				LOGGER.error("Exeption found in getParquteFilNameWithPath Method : {}", e);
			} finally {
				LOGGER.info("getParquteFilNameWithPath Method End..........");
			}
			return fileNameParqute;
		}
		/**
		 * 
		 * @param trancustFldDTOObj
		 * @return
		 */
		private String toFormParquetPathUtils(TransactionCustomFieldRDTO trancustFldDTOObj) {
			String destFileType = null;
			String destLocation = null;
			String sourceFileName = null;
			String source = null;
			String shortName = null;
			String rtnFilePath = null;
			try {
				 destFileType = trancustFldDTOObj.destFileType();
				 destLocation = trancustFldDTOObj.destLocation();
				 sourceFileName = trancustFldDTOObj.sourceFileName();
				 source = trancustFldDTOObj.source();
				 shortName =  trancustFldDTOObj.shortName();
				 if(StringUtils.isNotBlank(destLocation)) {////#shortname#.#DestfileType#
						// --c:/#ShortName#/#Source#/#year#/#month#/#date#/
						rtnFilePath = toRepalcePath(destLocation, shortName, source);

						LOGGER.debug("rtnFilePath ----------> {}", rtnFilePath);
						Path destinationDir = Paths.get(rtnFilePath);
						if (!Files.exists(destinationDir)) {
							Files.createDirectories(destinationDir);
							LOGGER.info("Created destination folder: [{}]", destinationDir);
						}
						rtnFilePath += shortName + "." + destFileType;
						LOGGER.info("rtnFilePath [IF] : {}", rtnFilePath);
				 } else {
					 LOGGER.info("rtnFilePath [ELSE] : {}", rtnFilePath);
				 }
			} catch(Exception e) {
				LOGGER.info("Exception found in toFormParquetPathUtils Method : {}", e);
			} finally {}
			return rtnFilePath;
		}

		/**
		 * 
		 * @param destLocation
		 * @param shortName
		 * @param source
		 * @return
		 */
		private String toRepalcePath(String destLocation, String shortName,String source) {
			
			Pattern p = Pattern.compile("#([^#]+)#");
			Matcher m = p.matcher(destLocation);

			Map<String, String> values = new HashMap<>();

			while (m.find()) {
			    String key = m.group(1);          // ShortName, Source, year, month, date, ...
			    values.put(key, null);            // initialize; value filled later
			}
			
			LocalDate today = LocalDate.now();
			String yearStr  = String.valueOf(today.getYear());
			String monthStr = String.format("%02d", today.getMonthValue());
			String dateStr  = String.format("%02d", today.getDayOfMonth());

			for (String key : values.keySet()) {
			    switch (key) {
			        case "ShortName":
			            values.put(key, shortName);
			            break;
			        case "shortname":
			            values.put(key, shortName.toLowerCase());
			            break;
			        case "Source":
			            values.put(key, source);
			            break;
			        case "year":
			            values.put(key, yearStr);
			            break;
			        case "month":
			            values.put(key, monthStr);
			            break;
			        case "date":
			            values.put(key, dateStr);
			            break;
			        default:
			            values.put(key, "");   // or some default
			    }
			}
			String result = destLocation;
			for (Map.Entry<String, String> e : values.entrySet()) {
			    String token = "#" + e.getKey() + "#";
			    result = result.replace(token, e.getValue());
			}

			return result;
		}
}