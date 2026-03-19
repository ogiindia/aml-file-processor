package com.aml.file.pro.core.efrmsrv.filewatcher.service;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.aml.file.pro.core.efrmsrv.utils.AMLConstants;
import com.aml.file.pro.core.efrmsrv.utils.CommonUtils;
import com.aml.file.pro.core.efrmsrv.utils.FileProcessProConfig;
import com.aml.file.pro.core.recordDTO.MapperSummarizationFiledImpl;

@Component
public class FileProcessService {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessService.class);

	@Autowired
	FileMapper fileMapper;
	
	@Autowired
	FileProcessProConfig fileProcessProConfig;
	
	@Autowired
	NRTFile2DataBaseUpateService nrtFileBsupdObj;
	
	@Autowired
	CommonUtils commonUtils;
	
	/*
	 * @Autowired ParquetFileConverterService parquetFileConverterService;
	 */
	
	 private final ParquetFileConverterService converterService;

	    // Constructor injection
	    public FileProcessService(ParquetFileConverterService converterService) {
	        this.converterService = converterService;
	    }

	/**
	 * 
	 * @param csvPathParam
	 * @param destinationPath
	 */
	public void csvfileProcessMethod(Path csvPathParam, String destinationPath) {
		LOGGER.info("FileProcessService@csvfileProcessMethod Called.........");
		Map<String, List<MapperSummarizationFiledImpl>> mapObj = null;
		Map<String, Object> mapOfFinaldata = null;
		List<MapperSummarizationFiledImpl> mappObjLst = null;
		List<Map<String, Object>> finalDataLst =  null;
		String fileName = null;
		String fileNamePrefix = null;
		Reader reader = null;
		CSVParser csvParser = null;
		try {
			if (csvPathParam != null) {
				mapOfFinaldata =  new HashMap<>();
				finalDataLst = new ArrayList<>();
				fileName = csvPathParam.getFileName().toString();
				fileNamePrefix = StringUtils.substringBefore(fileName, "_");

				//csvFileName = csvFilePathObj.getFileName().toString();
				LOGGER.info("CSV file Name is : [{}]",fileName);
				reader = new FileReader(csvPathParam.toAbsolutePath().toString());
				csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
				mapObj = fileMapper.toGetTranNRTMapfromRedis();
				LOGGER.debug("FileProcessService@csvfileProcessMethod - fileNamePrefix : [{}]", fileNamePrefix);
				if (mapObj != null && mapObj.size() > 0) {
					LOGGER.warn("Mapping Table MAP is available - Size : {}", mapObj.size());
					mappObjLst = mapObj.get(AMLConstants.NRTFILES + fileNamePrefix);
					if (mappObjLst != null && mappObjLst.size() > 0) {
						LOGGER.warn("Mapping Table MAP  is available - Size : {}", mapObj.size());
						Integer recordCount = 0;
						boolean listImplflg = false;	
						String amlTableName = null;
						String uniqueId = null;
						for (CSVRecord record : csvParser) {
							mapOfFinaldata =  new HashMap<>();
							for (MapperSummarizationFiledImpl mappSummFilDto : mappObjLst) {
								String csvColumnName = mappSummFilDto.getFieldName();
								if(StringUtils.isBlank(amlTableName)) { amlTableName = mappSummFilDto.getTableName(); }
								if(StringUtils.isBlank(uniqueId)) { uniqueId = mappSummFilDto.getWhereClauseCloumn();}
								String amlColumnName = mappSummFilDto.getColumnName();
								if(StringUtils.isNotBlank(amlColumnName)) {
									mapOfFinaldata.put(amlColumnName.toLowerCase(), record.get(csvColumnName));
								}
							}
							finalDataLst.add(mapOfFinaldata);
							LOGGER.info("recordCount : {} - finalDataLst - [{}]", recordCount, finalDataLst.size());
							if(recordCount == 1000 && finalDataLst!=null && finalDataLst.size()==1000) {
								toFindTableInsertUpd(amlTableName, finalDataLst, uniqueId);
								recordCount = 0;finalDataLst = new ArrayList<>();
								listImplflg = true;
							}
							recordCount++;
						}
						if(!listImplflg) {
							if (StringUtils.isNotBlank(amlTableName) && finalDataLst != null && finalDataLst.size() > 0) {
								toFindTableInsertUpd(amlTableName, finalDataLst,uniqueId);
							}
						} 
					} else { LOGGER.warn("Mapping Table is available - Size : {}", mapObj.size()); 	}
				} else { LOGGER.warn("Mapping Table is empty / not found"); }
			}
			if(reader!=null) { reader.close();reader=null; }
			if(csvParser!=null) { csvParser.close();csvParser=null; }
			
			String currentDateNmFldr = new SimpleDateFormat("yyyyMMdd").format(new Date());
			if(fileProcessProConfig.isIsmove()) {
				// To move into current data folder
				//Path toPath = Paths.get(DESTINATION_CSV_FOLDER +"/"+ currentDateNmFldr+"/", csvFileName);
				Path toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/");
				LOGGER.info("Before Create destination folder: {}", toPath);
				if (!Files.exists(toPath)) {
					Files.createDirectories(toPath);
					LOGGER.info("After Created destination folder: {}", toPath);
				}
				toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/", fileName);
				LOGGER.info("Completed from file path : {}", csvPathParam);
				LOGGER.info("Completed to file path : {}", toPath);
				commonUtils.toMove(csvPathParam, toPath);
			} else if(fileProcessProConfig.isIsdelete()) {
				LOGGER.info("Deleting file : {}", csvPathParam.toString());
				commonUtils.toDelete(csvPathParam.toString());
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in FileProcessService@csvfileProcessMethod Method : {}", e);
		} finally {
			if(reader!=null) { try { reader.close(); } catch (IOException e) { e.printStackTrace(); }reader=null; }
			if(csvParser!=null) { try { csvParser.close(); } catch (IOException e) { e.printStackTrace(); }csvParser=null;}
			mapObj = null; mapOfFinaldata = null;  mappObjLst = null; finalDataLst =  null;  fileName = null;  fileNamePrefix = null;
			LOGGER.info("FileProcessService@csvfileProcessMethod End.........");
		}
	}

	@Async
	private void toFindTableInsertUpd(String amlTableName, List<Map<String, Object>> finalDataLst, String uniqueId) {
		try {
			if (StringUtils.isNotBlank(amlTableName)) {
				amlTableName = amlTableName.toUpperCase();
				nrtFileBsupdObj.batchInsert(amlTableName, finalDataLst, uniqueId);
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in toFindTableInsertUpd Method : {}", e);
		} finally {

		}
	}
	
	/**
	 * 
	 * @param xlsxPathParam
	 * @param processCsvPathParam
	 */
	public void xlsProcess(Path xlsxPathParam, String processCsvPathParam) {
		LOGGER.info("FileProcessService@xlsProcess Method Called.........");
		String xlsxFileName = null; Workbook workbook = null; Sheet sheet = null; Row headerRow = null;
		Map<String, List<MapperSummarizationFiledImpl>> mapObj = null;
		List<MapperSummarizationFiledImpl> mappObjLst = null;
		List<Map<String, Object>> finalDataLst =  null;
		Map<String, Object> mapOfFinaldata = null;
		String fileName = null; String fileNamePrefix = null;
		Map<String, Integer> headerMap = null;
		try {
			
			mapOfFinaldata =  new HashMap<>();
			finalDataLst = new ArrayList<>();
			fileName = xlsxPathParam.getFileName().toString();
			fileNamePrefix = StringUtils.substringBefore(fileName, "_");
			LOGGER.debug("FileProcessService@csvfileProcessMethod - fileNamePrefix : [{}]", fileNamePrefix);
			mapObj = fileMapper.toGetTranNRTMapfromRedis();
			
			Path destinationDir = Paths.get(processCsvPathParam);
			if (!Files.exists(destinationDir)) {
				Files.createDirectories(destinationDir);
				LOGGER.info("Created destination folder: [{}]", destinationDir);
			}
			if (xlsxPathParam != null && Files.exists(xlsxPathParam) && Files.isRegularFile(xlsxPathParam)) {
				xlsxFileName = xlsxPathParam.getFileName().toString();
				LOGGER.info("Elcel XLSX file Name is : [{}]", xlsxFileName);
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
				
				if (mapObj != null && mapObj.size() > 0) {
					LOGGER.warn("Mapping Table MAP is available - Size : {}", mapObj.size());
					mappObjLst = mapObj.get(AMLConstants.NRTFILES + fileNamePrefix);
					if (mappObjLst != null && mappObjLst.size() > 0) {
						LOGGER.warn("Mapping Table MAP  is available - Size : {}", mapObj.size());
						Integer recordCount = 0;
						boolean listImplflg = false;	
						String amlTableName = null;
						String uniqueId = null;
						/*
						 * List<String> headers = new ArrayList<>(); headerRow.forEach(cell ->
						 * headers.add(cell.getStringCellValue()));
						 */
						headerMap = new HashMap<>();
						for (Cell cell : headerRow) {
							headerMap.put(cell.getStringCellValue().trim(), cell.getColumnIndex());
						}
						
						for (int i = 1; i <= sheet.getLastRowNum(); i++) {
							Row row = sheet.getRow(i);
							mapOfFinaldata = new HashMap<>();
							for (MapperSummarizationFiledImpl mappSummFilDto : mappObjLst) {
								String csvColumnName = mappSummFilDto.getFieldName();
								if (StringUtils.isBlank(amlTableName)) { amlTableName = mappSummFilDto.getTableName(); }
								if (StringUtils.isBlank(uniqueId)) { uniqueId = mappSummFilDto.getWhereClauseCloumn();}
								String amlColumnName = mappSummFilDto.getColumnName();
								if (StringUtils.isNotBlank(amlColumnName)) {
									 mapOfFinaldata.put(amlColumnName.toLowerCase(), row.getCell(headerMap.get(csvColumnName)));
								}
							}
							finalDataLst.add(mapOfFinaldata);
							LOGGER.info("recordCount : [{}] - finalDataLst - [{}]", recordCount, finalDataLst.size());
							if(recordCount == 1000 && finalDataLst!=null && finalDataLst.size()==1000) {
								toFindTableInsertUpd(amlTableName, finalDataLst, uniqueId);
								recordCount = 0;finalDataLst = new ArrayList<>();
								listImplflg = true;
							}
							recordCount++; row = null;
						}
						if(!listImplflg) {
							if (StringUtils.isNotBlank(amlTableName) && finalDataLst != null && finalDataLst.size() > 0) {
								toFindTableInsertUpd(amlTableName, finalDataLst,uniqueId);
							}
						}
					} else { LOGGER.warn("Mapping Table is available - Size : {}", mapObj.size());}
				}  else { LOGGER.warn("Mapping Table is empty / not found"); }
				
				/*
				 * sheet.forEach(row -> { if (row.getRowNum() == 0) return; csvColumnName});
				 */
				workbook.close(); workbook = null;
				
				String currentDateNmFldr = new SimpleDateFormat("yyyyMMdd").format(new Date());
				if(fileProcessProConfig.isIsmove()) {
					// To move into current data folder
					//Path toPath = Paths.get(DESTINATION_CSV_FOLDER +"/"+ currentDateNmFldr+"/", csvFileName);
					Path toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/");
					LOGGER.info("Before Create destination folder: {}", toPath);
					if (!Files.exists(toPath)) {
						Files.createDirectories(toPath);
						LOGGER.info("After Created destination folder: {}", toPath);
					}
					toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/", fileName);
					LOGGER.info("Completed from file path : {}", xlsxPathParam);
					LOGGER.info("Completed to file path : {}", toPath);
					commonUtils.toMove(xlsxPathParam, toPath);
				} else if(fileProcessProConfig.isIsdelete()) {
					LOGGER.info("Deleting file : {}", xlsxPathParam.toString());
					commonUtils.toDelete(xlsxPathParam.toString());
				}
				
			} else {
				LOGGER.info("Work Book Not Found : [{}]", workbook);
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in FileProcessService@xlsProcess : {}", e);
		} finally {
			xlsxFileName = null;workbook = null; sheet = null; headerRow = null;
			mapObj = null; mappObjLst = null; finalDataLst =  null;  mapOfFinaldata = null; 
			fileName = null; fileNamePrefix = null;  headerMap = null;
			LOGGER.info("FileProcessService@xlsProcess Method End.........");
		}
	}
	
	/**
	 * 
	 * @param filePathParam
	 * @param processCsvPathParam
	 */
	public void createParquteFiles(Path filePathParam, String processCsvPathParam) {
		LOGGER.info("FileProcessService@createParquteFiles Method Called.........");
		String currentDateNmFldr = new SimpleDateFormat("yyyyMMdd").format(new Date());
		String fileName = null;
		try {
			Path destinationDir = Paths.get(processCsvPathParam);
			if (!Files.exists(destinationDir)) {
				Files.createDirectories(destinationDir);
				LOGGER.info("Created destination folder: [{}]", destinationDir);
			}
			if (filePathParam != null && Files.exists(filePathParam) && Files.isRegularFile(filePathParam)) {
				fileName = filePathParam.getFileName().toString();
				LOGGER.info("Elcel XLSX file Name is : [{}]", filePathParam);
			}
			
			if (filePathParam.toString().endsWith(AMLConstants.XLSX_FORMAT)) {
				LOGGER.info("[XLSX] File is detected......");
				//converterService.convertExcelToParquet(filePathParam.toAbsolutePath().toString(), fileProcessProConfig.getParqutefilepath()+"/"+currentDateNmFldr);
			} else if (filePathParam.toString().endsWith(AMLConstants.XLS_FORMAT)) {
				LOGGER.info("[XLS] File is detected......");
				//converterService.convertExcelToParquet(filePathParam.toAbsolutePath().toString(), fileProcessProConfig.getParqutefilepath()+"/"+currentDateNmFldr);
				
			} else if (filePathParam.toString().endsWith(AMLConstants.CSV_FORMAT)) {
				LOGGER.info("[CSV] File is detected......");
				converterService.convertCsvToParquet(filePathParam.toAbsolutePath().toString(), fileProcessProConfig.getParqutefilepath()+"/"+currentDateNmFldr);
				
			} else if (filePathParam.toString().endsWith(AMLConstants.XML_FORMAT)) {
				LOGGER.info("[XML] File is detected......");
				//converterService.convertXmlToParquet(filePathParam.toAbsolutePath().toString(), fileProcessProConfig.getParqutefilepath()+"/"+currentDateNmFldr,fileProcessProConfig.getXmlroottag());
			} else {
				
			}
			
			
			if(fileProcessProConfig.isIsmove()) {
				// To move into current data folder
				//Path toPath = Paths.get(DESTINATION_CSV_FOLDER +"/"+ currentDateNmFldr+"/", csvFileName);
				Path toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/");
				LOGGER.info("Before Create destination folder: {}", toPath);
				if (!Files.exists(toPath)) {
					Files.createDirectories(toPath);
					LOGGER.info("After Created destination folder: {}", toPath);
				}
				toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/", fileName);
				LOGGER.info("Completed from file path : {}", filePathParam);
				LOGGER.info("Completed to file path : {}", toPath);
				commonUtils.toMove(filePathParam, toPath);
			} else if(fileProcessProConfig.isIsdelete()) {
				LOGGER.info("Deleting file : {}", filePathParam.toString());
				commonUtils.toDelete(filePathParam.toString());
			}
		} catch (Exception e) {
			LOGGER.error("Exception found in FileProcessService@createParquteFiles : {}",e);
		} finally {
			LOGGER.info("FileProcessService@createParquteFiles Method End.........");
		}
	}
}