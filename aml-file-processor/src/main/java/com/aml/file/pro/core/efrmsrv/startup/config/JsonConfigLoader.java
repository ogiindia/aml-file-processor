package com.aml.file.pro.core.efrmsrv.startup.config;

import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aml.file.pro.core.efrmsrv.config.RedisService;
import com.aml.file.pro.core.efrmsrv.utils.FileProcessProConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import jakarta.annotation.PostConstruct;

@Service
public class JsonConfigLoader {

	private static final Logger LOGGER = LoggerFactory.getLogger(JsonConfigLoader.class);

	private static final Gson gson = new GsonBuilder().create();
	
	@Autowired
	RedisService redisService;
	
	@Autowired
	FileProcessProConfig fileProcessProConfig;
	
	@PostConstruct
	public List<TransactionMapping> loadStartUpConfig() {
		LOGGER.info("JsonConfigLoader@loadStartUpConfig Method Called..........");
		 List<TransactionMapping> tranLtObj = null;
		try (Reader reader = new FileReader(fileProcessProConfig.getStartconfigpath())) {
			Type listType = new TypeToken<List<TransactionMapping>>() {}.getType();
			tranLtObj = gson.fromJson(reader, listType);
			
		    //tranLtObj = gson.fromJson(reader, TrasactionMppList.class);
			LOGGER.info("JsonConfigLoader@loadStartUpConfig Method - tranLtObj : {}", tranLtObj);
			redisService.toPushListIntoRedis("LDCONFIG", tranLtObj);
			
			
		} catch(Exception e) {
			LOGGER.error("Exception found in JsonConfigLoader@loadStartUpConfig : {}",e);
		} finally {
			LOGGER.info("JsonConfigLoader@loadStartUpConfig Method End..........");
		}
		return tranLtObj;
	}
	
	@SuppressWarnings("unchecked")
	public List<TransactionMapping> getStartUpConfig() {
		List<TransactionMapping> trnMapLst = null;
		try {
			trnMapLst = (List<TransactionMapping>) redisService.toPullObjectFrmRedis("LDCONFIG");
			LOGGER.info("getStartUpConfig method trnMapLst : {}", trnMapLst);
		} catch (Exception e) {
			LOGGER.info("Exception foud in getStartUpConfig Method : {}",e);
		}
		return trnMapLst;
	}
}
