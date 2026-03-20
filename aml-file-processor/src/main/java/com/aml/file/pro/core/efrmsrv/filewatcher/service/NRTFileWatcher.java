package com.aml.file.pro.core.efrmsrv.filewatcher.service;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.aml.file.pro.core.efrmsrv.config.PublishData2Kafka;
import com.aml.file.pro.core.efrmsrv.utils.AMLConstants;
import com.aml.file.pro.core.efrmsrv.utils.CommonUtils;
import com.aml.file.pro.core.efrmsrv.utils.FileProcessProConfig;
import com.aml.file.pro.core.recordDTO.FinSecIndicatorVO;

import jakarta.annotation.PostConstruct;

@Component
public class NRTFileWatcher {

	private static final Logger LOGGER = LoggerFactory.getLogger(NRTFileWatcher.class);

	@Autowired
	FileProcessProConfig fileProcessProConfig;

	@Autowired
	FLTtoCSVConverter converter;
	
	@Autowired
	CommonUtils commonUtils;
	
	@Autowired
	AMLDataTblDetailsFetch amlDataTblDetailsFetch;
	
	@Autowired
	PublishData2Kafka publishData2Kafka;
	
	@Autowired
	FileProcessService fileProcessService;
	
	@Autowired
	FileMapper fileMapper;
	
	Long startDateMain = new Date().getTime();
	
	

	@PostConstruct
	void nrtTransFileWatcher() {
		System.setProperty("parquet.columnwriter.version", "v1");   // force old writer
	    System.setProperty("parquet.statistics.enabled", "false");  // disable stats entirely
	    System.setProperty("parquet.bloom.filter.enabled", "false"); // extra safety

	    // start Spring / your app...
		LOGGER.info("-------------------START");
		try {
			Thread thread = new Thread(() -> {
				boolean completedFileCountSts=false;
				try {
					WatchService watchService = FileSystems.getDefault().newWatchService();
					Path path = Paths.get(fileProcessProConfig.getSourcepath());
					path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);// , // StandardWatchEventKinds.ENTRY_MODIFY);//														// StandardWatchEventKinds.ENT
					LOGGER.info("=============Processing started==================");
					LOGGER.info("Watching directory: {}", fileProcessProConfig.getSourcepath());

					while (true) {
						WatchKey key = watchService.take();
						List<WatchEvent<?>> watcheventss = key.pollEvents();
						Integer fileCount = watcheventss.size();
						for (WatchEvent<?> event : watcheventss) {
							WatchEvent.Kind<?> kind = event.kind();
							Path fileName = (Path) event.context();
							LOGGER.info("kind : {}", kind);
							if (!StandardWatchEventKinds.ENTRY_DELETE.equals(kind) && !StandardWatchEventKinds.ENTRY_MODIFY.equals(kind)) {
								if (fileName != null && fileName.toString().endsWith(fileProcessProConfig.getFromfileformat())) {
									LOGGER.info("File format {} Block", fileProcessProConfig.getFromfileformat());
									Path fullPath = path.resolve(fileName);
									// Waiting for file write completion
									waitForFileCompletion(fullPath);

									String csvNewFilename = converter.convertFLTToCsv(fullPath, fileProcessProConfig.getDestinationpath(), fileProcessProConfig.getProcessedpath());
									if (StringUtils.isNotBlank(csvNewFilename)) {
										Path csvFilePath = Paths.get(fileProcessProConfig.getDestinationpath(), csvNewFilename);
										if (csvFilePath != null && Files.exists(csvFilePath)) {
											LOGGER.info("Final CSV Path and Name after Convert : {}", csvFilePath.toString());
											Long startDate = new Date().getTime();
											LOGGER.info("NRTFileWatcher CSV Import Start Time : [{}]", startDate);
											
											// process Direct Insert
											//fileProcessService.csvfileProcessMethod(csvFilePath, fileProcessProConfig.getDestinationpath());
											
											Long endTime = new Date().getTime();
											LOGGER.info("NRTFileWatcher CSV Import - Total time : {}", commonUtils.findIsHourMinSec((endTime - startDate)));
										}
									}

								} else if (fileName != null && fileName.toString().endsWith(AMLConstants.CSV_FORMAT)) {
									LOGGER.info("File format {} Block", fileProcessProConfig.getFromfileformat());
									Path fullPath = path.resolve(fileName);
									// Waiting for file write completion
									waitForFileCompletion(fullPath);

									String toCsvFileName = fileName.getFileName().toString();
									Path csvFilePath = Paths.get(fileProcessProConfig.getDestinationpath(), toCsvFileName);
									commonUtils.toMove(fullPath, csvFilePath);
									LOGGER.info("###############File moved successfully#############");
									
									try {
										if (StringUtils.isNotBlank(toCsvFileName)) {
											if (csvFilePath != null && Files.exists(csvFilePath)) {
												LOGGER.info("Final CSV Path and Name after Convert : [{}]", csvFilePath.toString());
												Long startDate = new Date().getTime();
												LOGGER.info("NRTFileWatcher CSV Import Start Time : [{}]", startDate);

												// process Direct Insert
												//fileProcessService.csvfileProcessMethod(csvFilePath, fileProcessProConfig.getDestinationpath());
												// process using parqute file
												fileProcessService.createParquteFiles(csvFilePath, fileProcessProConfig.getDestinationpath());
												Long endTime = new Date().getTime();
												LOGGER.info("NRTFileWatcher CSV Import - Total time : [{}]", commonUtils.findIsHourMinSec((endTime - startDate)));
											}
										}
									} catch (Exception e) {
										LOGGER.error("Exception found in watchDirectory : {}", e);
									}
									LOGGER.info("File format {} block End.", AMLConstants.CSV_FORMAT);
								} else if (fileName != null && (fileName.toString().endsWith(AMLConstants.XLS_FORMAT) 
										|| fileName.toString().endsWith(AMLConstants.XLSX_FORMAT))) {
									
									LOGGER.info("File format {} Block Called.", fileProcessProConfig.getFromfileformat());
									Path fullPath = path.resolve(fileName);
									// Waiting for file write completion
									waitForFileCompletion(fullPath);

									String toCsvFileName = fileName.getFileName().toString();
									Path csvFilePath = Paths.get(fileProcessProConfig.getDestinationpath(), toCsvFileName);
									commonUtils.toMove(fullPath, csvFilePath);
									LOGGER.info("###############File moved successfully#############");
									
									try {
										if (StringUtils.isNotBlank(toCsvFileName)) {
											if (csvFilePath != null && Files.exists(csvFilePath)) {
												LOGGER.info("Final XLS/XLSX Path and Name after Convert : [{}]", csvFilePath.toString());
												Long startDate = new Date().getTime();
												LOGGER.info("NRTFileWatcher XLS/XLSX Import Start Time : [{}]", startDate);

												// process Direct Insert
												//fileProcessService.xlsProcess(csvFilePath, fileProcessProConfig.getDestinationpath());
												// process using parqute file
												fileProcessService.createParquteFiles(csvFilePath, fileProcessProConfig.getDestinationpath());
												Long endTime = new Date().getTime();
												LOGGER.info("NRTFileWatcher XLS/XLSX Import - Total time : [{}]", commonUtils.findIsHourMinSec((endTime - startDate)));
											}
										}
									} catch (Exception e) { LOGGER.error("Exception found in watchDirectory : {}", e); }
									LOGGER.info("File format {} block End.", AMLConstants.XLSX_FORMAT +"/" + AMLConstants.XLS_FORMAT);
								}
							
								completedFileCountSts = packageWatcherToChkFileCntReached();
								LOGGER.info("completedFileCountSts - [{}]",completedFileCountSts);
								if (completedFileCountSts) {
									FinSecIndicatorVO finSecIndicatorVOoBj = new FinSecIndicatorVO();
									finSecIndicatorVOoBj = amlDataTblDetailsFetch
											.toSetFinSecIndicatorObjectForDuckDBSts(finSecIndicatorVOoBj);
									finSecIndicatorVOoBj = amlDataTblDetailsFetch
											.toGetRowCountEachAMLTblSetINFincSecIndicator(finSecIndicatorVOoBj);
								
									publishData2Kafka.sendtoKafka(finSecIndicatorVOoBj.getUuid(), finSecIndicatorVOoBj, AMLConstants.KAFKA_PUB_TOPIC);
									
									Long endTime = new Date().getTime();
									LOGGER.info("Total file processed time : {}", commonUtils.findIsHourMinSec((endTime - startDateMain)));	
								}
								// File fetch interval on each.
								Thread.sleep(fileProcessProConfig.getFetchinterval());
							}
						}
						boolean valid = key.reset(); if (!valid) { break; }
					}
				} catch (Exception e) {
					LOGGER.info("Exception found in NRTFileWatcher@nrtTransFileWatcher [INSIDE CATCH]: {}", e);
				} finally { }
			});
			thread.setDaemon(true);
			thread.start();
		} catch (Exception e) {
			LOGGER.info("Exception found in NRTFileWatcher@nrtTransFileWatcher [OUTSIDE CATCH]: {}", e);
		} finally { }
	}

	/**
	 * 
	 * @param file
	 * @throws InterruptedException waitForFileCompletion void
	 */
	private void waitForFileCompletion(Path file) throws InterruptedException {
		long previousSize = -1;
		while (true) {
			long currentSize = file.toFile().length();
			if (currentSize == previousSize)
				break;
			previousSize = currentSize;
			try {
				// File Coppied Completion Check
				Thread.sleep(fileProcessProConfig.getCompletioncheckinterval());
			} catch (InterruptedException e) {
			} // Wait and check again
		}
	}
	
	/**
	 * 
	 * @return
	 * packageWatcherToChkFileCntReached
	 * boolean
	 */
	private boolean packageWatcherToChkFileCntReached() {
		LOGGER.info("::::::::::::NRTFileWatcher@packageWatcherToChkFileCntReached methos called.:::::");
		String currentDateNmFldr = null;
		Path toPath = null;
		boolean cbsFileImportStatus = false;
		long count = 0;
		try {
			currentDateNmFldr = new SimpleDateFormat("yyyyMMdd").format(new Date());
			toPath = Paths.get(fileProcessProConfig.getProcessedpath() + "/" + currentDateNmFldr + "/");
			
			if (!Files.exists(toPath)) { Files.createDirectories(toPath); LOGGER.info("After Created destination folder: {}", toPath); }
			
			LOGGER.info("Get File Count - CSV/XLS/XLSX Folder Path : [{}]",toPath);
			// while(true) {
			if (toPath != null) {
				count = Files.list(toPath).filter(Files::isRegularFile).count();
				if (count == 1) {
					startDateMain = new Date().getTime();
				}
				LOGGER.info("Config / Required Count is : [{}], File Count : [{}]", fileProcessProConfig.getFilecount(), count);
				if (count == fileProcessProConfig.getFilecount()) {
					cbsFileImportStatus = true;
					// break;
				}
			} // Thread.sleep(6000);}
		} catch (Exception e) {
			LOGGER.error("Exception found in NRTFileWatcher@fileWatcherTogetCount : {}", e);
		} finally {  currentDateNmFldr = null; toPath = null; }
		LOGGER.info("::::::::::::NRTFileWatcher@packageWatcherToChkFileCntReached method end.:::::\n");
		return cbsFileImportStatus;
	}
}
