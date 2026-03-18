package com.aml.file.pro.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;

/**
 * Hello world!
 *
 */
@EnableEncryptableProperties
@EntityScan({ "com.aml.file.pro.core" })
@ComponentScan({ "com.aml.file.pro.core" })
@EnableJpaRepositories({ "com.aml.file.pro.core" })
@SpringBootApplication
@EnableScheduling
@EnableAutoConfiguration
@ComponentScan
public class AMLFileProcessorApp extends SpringBootServletInitializer{// implements CommandLineRunner

	public static void main(String[] args) {
		SpringApplication.run(AMLFileProcessorApp.class, args);
		System.out.println("Hello World!");
	}

	/*
	 * @Override public void run(String... args) throws Exception {
	 * System.out.println("=== Application Started ==="); }
	 */
}
