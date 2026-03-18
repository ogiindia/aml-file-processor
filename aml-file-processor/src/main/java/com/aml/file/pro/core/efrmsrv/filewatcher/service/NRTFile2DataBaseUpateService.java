package com.aml.file.pro.core.efrmsrv.filewatcher.service;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.aml.file.pro.core.efrmsrv.entity.AccountDetailsEntity;
import com.aml.file.pro.core.efrmsrv.repo.AccountDetailsRepositry;
import com.aml.file.pro.core.efrmsrv.utils.DateFormatUtils;
import com.aml.file.pro.core.efrmsrv.utils.FileProcessProConfig;

import jakarta.transaction.Transactional;

@Component
public class NRTFile2DataBaseUpateService {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessService.class);
	
	@Autowired
	AccountDetailsRepositry<?> accountDetailsRepositry;
	
	@Autowired
	DateFormatUtils dateFormatUtils;
	
	@Autowired
	FileProcessProConfig fileProcessProConfig;
	
	@Value("${spring.jpa.properties.hibernate.default_schema:amlschema}")
	private String schemaName;
	
	private final JdbcTemplate jdbcTemplate;

    public NRTFile2DataBaseUpateService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
	public void batchInsert(String tableName, List<Map<String, Object>> records, String uniqueId) {
        if (records.isEmpty()) return;

        String columns = String.join(", ", records.get(0).keySet());
        String placeholders = records.get(0).keySet().stream()
                .map(k -> "?")
                .collect(Collectors.joining(", "));
		//String sql = "INSERT INTO " + schemaName + "." + tableName + " (" + columns + ") VALUES (" + placeholders + ")";
        String sql = null;
      /*  jdbcTemplate.batchUpdate(sql, records, 50, (ps, record) -> {
            int i = 1;
            for (Object value : record.values()) {
                ps.setObject(i++, value);
            }
        });*///String schemaName, String tableName, String columns, String uniqueKey, String dbType
		sql = buildUpsertSql(schemaName, tableName, columns, uniqueId, fileProcessProConfig.getDbtype());
		
        LOGGER.debug("sql -->: {}", sql);
        
        jdbcTemplate.batchUpdate(sql, records, fileProcessProConfig.getBatchcount(), (ps, record) -> {
            int i = 1;
            for (Map.Entry<String,Object> entry : record.entrySet()) {
                Object value = entry.getValue();

                if (value == null) {
                    ps.setObject(i++, null);
                } else if (value instanceof String) {
                    String str = (String) value;
                    // Try to convert dynamically
                    if (str.matches("\\d+")) {
                        ps.setLong(i++, Long.parseLong(str));
                    } else if (str.matches("\\d+\\.\\d+")) {
                        ps.setBigDecimal(i++, new java.math.BigDecimal(str));
                    } else if (dateFormatUtils.isDate(str)) {
                        ps.setTimestamp(i++, dateFormatUtils.toTimestamp(str));
                    } else {
                        ps.setString(i++, str);
                    }
                } else if (value instanceof Integer) {
                    ps.setInt(i++, (Integer) value);
                } else if (value instanceof Long) {
                    ps.setLong(i++, (Long) value);
                } else if (value instanceof java.math.BigDecimal) {
                    ps.setBigDecimal(i++, (java.math.BigDecimal) value);
                } else if (value instanceof java.sql.Timestamp) {
                    ps.setTimestamp(i++, (java.sql.Timestamp) value);
                } else {
                    ps.setObject(i++, value);
                }
            }
        });
    }
	
	/**
	 * Not Use, will Check
	 * @param <T>
	 * @param map
	 * @param clazz
	 * @return
	 */
	public static <T> T mapToEntity(Map<String, Object> map, Class<T> clazz) {
        try {
            T entity = clazz.getDeclaredConstructor().newInstance();

            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                for (Field field : clazz.getDeclaredFields()) {
                    if (field.getName().equalsIgnoreCase(key)) {
                        field.setAccessible(true);

                        // Handle conversions
                        if (field.getType().equals(BigDecimal.class) && value != null) {
                            field.set(entity, new BigDecimal(value.toString()));
                        } else if (field.getType().equals(Timestamp.class) && value != null) {
                            field.set(entity, Timestamp.valueOf(value.toString()));
                        } else {
                            field.set(entity, value);
                        }
                    }
                }
            }
            return entity;
        } catch (Exception e) {
            throw new RuntimeException("Error mapping to entity", e);
        }
    }
	
	/**
	 * 
	 * @param map
	 * @param key
	 * @return
	 */
	public static Long getLongValue(Map<String, String> map, String key) {
        Object value = map.getOrDefault(key, null);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            // Optionally log or handle invalid format
            System.out.println("Invalid number format for key: " + key + " value: " + value);
            return null;
        }
    }
	
	
	public static String buildUpsertSql(String schemaName, String tableName, String columns, String uniqueKey,
			String dbType) {
		List<String> colList = Arrays.stream(columns.split(",")).map(String::trim).collect(Collectors.toList());

		String placeholders = colList.stream().map(c -> "?").collect(Collectors.joining(", "));

		String updateClause;
		if ("mysql".equalsIgnoreCase(dbType)) {
			updateClause = colList.stream().filter(c -> !c.equalsIgnoreCase(uniqueKey))
					.map(c -> c + " = VALUES(" + c + ")").collect(Collectors.joining(", "));
			return "INSERT INTO " + schemaName + "." + tableName + " (" + columns + ") VALUES (" + placeholders + ") "
					+ "ON DUPLICATE KEY UPDATE " + updateClause;
		} else if ("postgres".equalsIgnoreCase(dbType) || "duckdb".equalsIgnoreCase(dbType)) {
			updateClause = colList.stream().filter(c -> !c.equalsIgnoreCase(uniqueKey)).map(c -> c + " = EXCLUDED." + c)
					.collect(Collectors.joining(", "));
			return "INSERT INTO " + schemaName + "." + tableName + " (" + columns + ") VALUES (" + placeholders + ") "
					+ "ON CONFLICT (" + uniqueKey + ") DO UPDATE SET " + updateClause;
		} else if ("oracle".equalsIgnoreCase(dbType)) {
		        // Build update clause for MERGE
		        updateClause = colList.stream().filter(c -> !c.equalsIgnoreCase(uniqueKey)).map(c -> "target." + c + " = source." + c)
		                        .collect(Collectors.joining(", "));

		        // Build insert clause
		        String insertCols = String.join(", ", colList);
		        String insertVals = colList.stream().map(c -> "source." + c).collect(Collectors.joining(", "));

		        return "MERGE INTO " + schemaName + "." + tableName + " target " +
		               "USING (SELECT " + placeholders + " FROM dual) source (" + columns + ") " +
		               "ON (target." + uniqueKey + " = source." + uniqueKey + ") " +
		               "WHEN MATCHED THEN UPDATE SET " + updateClause + " " +
		               "WHEN NOT MATCHED THEN INSERT (" + insertCols + ") VALUES (" + insertVals + ")";
			} else {

				throw new IllegalArgumentException("Unsupported DB type: " + dbType);
			}
	}
}
