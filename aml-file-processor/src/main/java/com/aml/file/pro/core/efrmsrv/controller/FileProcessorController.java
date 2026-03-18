package com.aml.file.pro.core.efrmsrv.controller;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.aml.file.pro.core.efrmsrv.config.PublishData2Kafka;
import com.aml.file.pro.core.efrmsrv.filewatcher.service.AMLDataTblDetailsFetch;
import com.aml.file.pro.core.efrmsrv.filewatcher.service.FLTtoCSVConverter;
import com.aml.file.pro.core.efrmsrv.utils.AMLConstants;
import com.aml.file.pro.core.efrmsrv.utils.CommonUtils;
import com.aml.file.pro.core.recordDTO.FinSecIndicatorVO;

@RestController
@RequestMapping({ "/api/v1/" })
public class FileProcessorController {

	private Logger LOGGER = LoggerFactory.getLogger(FileProcessorController.class);

	
	@Autowired
	FLTtoCSVConverter converter;
	
	@Autowired
	AMLDataTblDetailsFetch amlDataTblDetailsFetch;
	
	@Autowired
	PublishData2Kafka publishData2Kafka;

	
	@Autowired
	CommonUtils commonUtils;
	
	@RequestMapping(value = { "post/fileupload" }, method = { RequestMethod.POST })
	public ResponseEntity<?> postFileUpload() {

		Long startDateMain = new Date().getTime();
		ResponseEntity<Object> retunRespEntity = null;
		
		try {

			FinSecIndicatorVO finSecIndicatorVOoBj = new FinSecIndicatorVO();
			finSecIndicatorVOoBj = amlDataTblDetailsFetch
					.toSetFinSecIndicatorObjectForDuckDBSts(finSecIndicatorVOoBj);
			finSecIndicatorVOoBj = amlDataTblDetailsFetch
					.toGetRowCountEachAMLTblSetINFincSecIndicator(finSecIndicatorVOoBj);
			/*
			 * finSecIndicatorVOoBj = customerProfiling
			 * .addCustomerProfilingStsFinSecIndictor(finSecIndicatorVOoBj);
			 */
			publishData2Kafka.sendtoKafka(finSecIndicatorVOoBj.getUuid(), finSecIndicatorVOoBj,
					AMLConstants.KAFKA_PUB_TOPIC);
			
			Long endTime = new Date().getTime();
			LOGGER.info("Total file processed time : {}", commonUtils.findIsHourMinSec((endTime - startDateMain)));
			
		
		} catch (Exception e) {
			LOGGER.error("Exception found in RuleValidationController@ruleProcessMethod : {}", e);
			retunRespEntity = getResponseEntity("Exception, Will check with Admin", HttpStatus.BAD_REQUEST);
		} finally {

		}
		return retunRespEntity;

	}

	public ResponseEntity<Object> getResponseEntity(String respMsg, HttpStatus httpStatus) {
		return new ResponseEntity<Object>(respMsg, httpStatus);
	}
	
	
	
	//@PostConstruct
	void toStartPush() {
		Long startDateMain = new Date().getTime();
		try {
			LOGGER.info(":::::::::::::::: toStartPush Method Called ::::::::::");
			FinSecIndicatorVO finSecIndicatorVOoBj = new FinSecIndicatorVO();
			finSecIndicatorVOoBj = amlDataTblDetailsFetch.toSetFinSecIndicatorObjectForDuckDBSts(finSecIndicatorVOoBj);
			finSecIndicatorVOoBj = amlDataTblDetailsFetch
					.toGetRowCountEachAMLTblSetINFincSecIndicator(finSecIndicatorVOoBj);
			//finSecIndicatorVOoBj = customerProfiling.addCustomerProfilingStsFinSecIndictor(finSecIndicatorVOoBj);
			publishData2Kafka.sendtoKafka(finSecIndicatorVOoBj.getUuid(), finSecIndicatorVOoBj,
					AMLConstants.KAFKA_PUB_TOPIC);
			Long endTime = new Date().getTime();
			LOGGER.info("Total file processed time : {}", commonUtils.findIsHourMinSec((endTime - startDateMain)));
		} catch (Exception e) {
			LOGGER.error("Exception found in RuleValidationController@toStartPush : {}", e);
			//retunRespEntity = getResponseEntity("Exception, Will check with Admin", HttpStatus.BAD_REQUEST);
		} finally {

		}
	}
}