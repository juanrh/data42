package com.github.juanrh.data42;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;

public interface Configured {
	public static class Utils {
		private static final Logger LOGGER = LoggerFactory.getLogger(Configured.class);
		/**
		 * Modify configured by calling the setters corresponding to 
		 * those keys that appear in conf and that have been annotated
		 * in the class for configured by using @Config
		 * */
		public static void loadConf(Configuration conf, Object configured) {
			CaseFormat lowerCamelFormat = CaseFormat.valueOf(CaseFormat.LOWER_CAMEL.name());
			CaseFormat upperUnderscoreFormat = CaseFormat.valueOf(CaseFormat.UPPER_UNDERSCORE.name());
			
			Class<?> configuredClass = configured.getClass();
			Field[] fields = configuredClass.getDeclaredFields();
			for (Field field : fields) {
				if (field.isAnnotationPresent(Config.class)) {
					Config confAnnotation = field.getAnnotation(Config.class);
					String key = confAnnotation.value();
					if (key.equals("")) {
						// use the key name converted to THIS_FORMAT as key
						// NOTE: this only works for fields in lower CamelCase
						key = lowerCamelFormat.to(upperUnderscoreFormat, field.getName()); 
					}
				
					if (conf.containsKey(key)) {
						try {
							String methodName = "set" + 
									field.getName().substring(0, 1).toUpperCase() +
									field.getName().substring(1); 						
							Method setter = configuredClass.getMethod(methodName, field.getType());
							setter.invoke(configured, conf.getProperty(key));
						} catch (NoSuchMethodException | SecurityException |
								 IllegalAccessException | IllegalArgumentException | 
								 InvocationTargetException e) {
							LOGGER.error("Exception configuring value {} with configuration : {}",
									configured, conf, ExceptionUtils.getFullStackTrace(e));
							throw new RuntimeException(e);
						}
					}					
				}
			}
		}
		
		// http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/base/CaseFormat.html
	}
}
