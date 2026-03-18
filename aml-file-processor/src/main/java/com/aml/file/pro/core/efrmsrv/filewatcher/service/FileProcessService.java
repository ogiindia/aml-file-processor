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
				LOGGER.info("FileProcessService@csvfileProcessMethod - fileNamePrefix : [{}]", fileNamePrefix);
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
								if (amlTableName == null) {
									amlTableName = mappSummFilDto.getTableName();
								}
							
								if(StringUtils.isBlank(uniqueId)) {
									uniqueId = mappSummFilDto.getWhereClauseCloumn();
								}
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
						
					} else {
						LOGGER.warn("Mapping Table is available - Size : {}", mapObj.size());
					}

				} else {
					LOGGER.warn("Mapping Table is empty / not found");
				}
			}
			if(reader!=null) {
				reader.close();reader=null;
			}
			if(csvParser!=null) {
				csvParser.close();csvParser=null;
			}
			
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
		
			/*
			 * String fileNamPrfix = null; if(StringUtils.isNotBlank(fileName)) {
			 * if(fileName.contains("_")) { String spltFN[] = fileName.split("_");
			 * if(spltFN!=null && spltFN.length>0) { fileNamPrfix = spltFN[0]; } } }
			 */
		} catch (Exception e) {
			LOGGER.error("Exception found in FileProcessService@csvfileProcessMethod Method : {}", e);
		} finally {
			if(reader!=null) {
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}reader=null;
			}
			if(csvParser!=null) {
				try {
					csvParser.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}csvParser=null;
			}
			LOGGER.info("FileProcessService@csvfileProcessMethod End.........");
		}
	}

	@Async
	private void toFindTableInsertUpd(String amlTableName, List<Map<String, Object>> finalDataLst, String uniqueId) {
		try {
			if (StringUtils.isNotBlank(amlTableName)) {
				amlTableName = amlTableName.toUpperCase();
				nrtFileBsupdObj.batchInsert(amlTableName, finalDataLst, uniqueId);
				/*
				 * switch (amlTableName) { case "FS_ACCOUNT_DETAILS":
				 * //nrtFileBsupdObj.accountDetailsUpdate(finalDataLst);
				 * nrtFileBsupdObj.batchInsert("FS_ACCOUNT_DETAILS", finalDataLst); break; case
				 * "FS_ACCOUNT_FEATURES": break; case "FS_ACCOUNT_IMPORT": break; case
				 * "FS_ACCOUNT_STATUS": break; case "FS_BRANCH": break; case
				 * "FS_CBWT_TRN_REPORT": break; case "FS_CHEQUE": break; case "FS_CNTRY": break;
				 * case "FS_CURM": break; case "FS_CUST": break; case "FS_INSTRUMENTS": break;
				 * case "FS_JTH": break; case "FS_LCKR": break; case "FS_MAB": break; case
				 * "FS_MINOR": break; case "FS_NCUST": break; case "FS_NOM": break; case
				 * "FS_PRD": break; case "FS_TRANS_TYPE": break; case "FS_TRANSACTION_FEATURE":
				 * break; case "FS_TRN": break; default: break; }
				 */
			}

		} catch (Exception e) {
			LOGGER.error("Exception found in toFindTableInsertUpd Method : {}", e);
		} finally {

		}
	}

}
