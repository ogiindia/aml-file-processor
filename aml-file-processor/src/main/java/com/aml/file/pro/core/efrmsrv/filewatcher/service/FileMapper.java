package com.aml.file.pro.core.efrmsrv.filewatcher.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.aml.file.pro.core.efrmsrv.config.RedisService;
import com.aml.file.pro.core.efrmsrv.repo.MapperImpl;
import com.aml.file.pro.core.efrmsrv.utils.AMLConstants;
import com.aml.file.pro.core.efrmsrv.utils.FileProcessProConfig;
import com.aml.file.pro.core.recordDTO.MapperSummarizationFiledImpl;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Component
public class FileMapper {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileMapper.class);
	
	@Autowired   
	FileProcessProConfig fileProcessProConfig;
	
	@Autowired
	MapperImpl mapperImpl;
	
	@Autowired
	RedisService redisService;
	
	String redisKey = "NRTFILES-MAP";
	
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	
	/**
	 * 
	 */
	@Bean
	public void toLoadTranNRTMaptoRedis(){
		LOGGER.info("FileMapper@toLoadTranNRTMaptoRedis Method Called...............");
		String txnTableSPlt = null;
		String tablePrefiKey = null;
		Map<String, List<MapperSummarizationFiledImpl>> txnTblLstOj = null;
		try {
			txnTableSPlt = fileProcessProConfig.getTxntablenameprefix();
			LOGGER.info("FileMapper@toLoadTranNRTMaptoRedis txnTableSPlt : [{}]",txnTableSPlt);
			txnTblLstOj = new HashMap<>();
			if(StringUtils.isNotBlank(txnTableSPlt) && txnTableSPlt.contains(",")) {
				LOGGER.info("txnTableSPlt[ IF] : {}",txnTableSPlt);
				List<String> amlFileTblNamelstObj = new ArrayList<>(Arrays.asList(txnTableSPlt.split(",")));
				for(String tbPrx : amlFileTblNamelstObj) {
					tablePrefiKey = AMLConstants.NRTFILES + tbPrx;
					//String fieldName, String tableName,String columnName, String whereClauseCloumn
					List<MapperSummarizationFiledImpl> dtoList = mapperImpl.getMapperByIdentifier(tablePrefiKey).stream()
						    .map(p -> new MapperSummarizationFiledImpl(p.getFieldName(),p.getTableName(), p.getColumnName(), p.getWhereClauseCloumn()))
						    .collect(Collectors.toList());
					
					txnTblLstOj.put(tablePrefiKey, dtoList);
				}
				redisService.toPushListIntoRedis(redisKey, txnTblLstOj);
			} else {
				LOGGER.info("txnTableSPlt [ELSE] : {}",txnTableSPlt);
			}			
		} catch (Exception e) {
			LOGGER.info("Exception found in FileMapper@toLoadTranNRTMaptoRedis : {}",e);
		} finally {
			txnTableSPlt = null;  tablePrefiKey = null; txnTblLstOj = null;
		}
	}
	/**
	 * 
	 * @return
	 */
	public Map<String, List<MapperSummarizationFiledImpl>> toGetTranNRTMapfromRedis(){
		Map<String, List<MapperSummarizationFiledImpl>> mapObj = null;
		try {
			mapObj = (Map<String, List<MapperSummarizationFiledImpl>>) redisService.toPullObjectFrmRedis(redisKey);
		} catch (Exception e) {
			LOGGER.info("Exception found in FileMapper@toGetTranNRTMapfromRedis : {}",e);
		} finally { }
		return mapObj;
	}
	
	
	@PostConstruct
	public void startJob() {
		executor.scheduleAtFixedRate(() -> {
			try {
				// Example: read/write to Redis
				redisService.setValue("heartbeat", System.currentTimeMillis());
				LOGGER.warn("Redis heartbeat updated");
			} catch (Exception e) {
				// Handle gracefully if Redis is unavailable
				LOGGER.error("Redis job failed: " + e.getMessage());
			}
		}, 0, 60, TimeUnit.SECONDS);
	}

	@PreDestroy
	public void stopJob() {
		// Cleanly shut down when Spring context closes
		executor.shutdownNow();
		LOGGER.warn("Redis scheduled job stopped");
	}
}
