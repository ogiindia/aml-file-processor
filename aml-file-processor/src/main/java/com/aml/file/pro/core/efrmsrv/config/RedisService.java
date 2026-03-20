package com.aml.file.pro.core.efrmsrv.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class RedisService {

	private static final Logger Logger = LoggerFactory.getLogger(RedisService.class);

	private RedisTemplate<String, Object> redisTemplate;
	

	public RedisService(RedisTemplate<String, Object> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}
	
	public Object getValue(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public void setValue(String key, Object value) {
        redisTemplate.opsForValue().set(key, value);
    }
    
	/*
	 * public <T> T toPullObjectFrmRedis(String key, Class<T> type) { Gson gson =
	 * new Gson(); String json = (String) redisTemplate.opsForValue().get(key);
	 * return gson.fromJson(json, type); }
	 */
   

	public Object toPullObjectFrmRedis(String keyName) {
		try {
			return (Object) redisTemplate.opsForValue().get(keyName);
		} catch (Exception e) {
			Logger.error("Exception found in RedisService@toPullObjectFrmRedis : {}", e);
			return null;
		} finally {

		}
	}

	public String toPushListIntoRedis(String keyName, Object objParam) {
		try {

			redisTemplate.opsForValue().set(keyName, objParam);

		} catch (Exception e) {
			Logger.error("Exception found in RedisService@toPushListIntoRedis : {}", e);
		} finally {

		}
		return "SUCCESS";
	}

	
}
