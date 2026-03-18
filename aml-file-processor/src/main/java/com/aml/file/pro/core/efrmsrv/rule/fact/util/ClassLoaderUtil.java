package com.aml.file.pro.core.efrmsrv.rule.fact.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class ClassLoaderUtil {

	private Logger LOGGER = LoggerFactory.getLogger(ClassLoaderUtil.class);

	@Autowired
	private ApplicationContext context;

	public <T> T getBean(String beanName, Class<T> clazz) {
		if (context.containsBean(beanName))
			return context.getBean(beanName, clazz);
		else
			return context.getBean(beanName, clazz);

	}

}
