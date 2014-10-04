package com.github.juanrh.data42;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Configured<Keys extends Enum<?>> {
	public static class Utils {
		private static final Logger LOGGER = LoggerFactory.getLogger(Configured.class);
		/**
		 * Modify configured by calling the setters corresponding to 
		 * those keys that appear in conf and that have been annotated
		 * in the class for configured by using @Config
		 * */
		public static <Keys extends Enum<?>>
		   void loadConf(Configuration conf, Configured<Keys> configured) {
			Class<?> configuredClass = configured.getClass();
			Field[] fields = configuredClass.getDeclaredFields();
			for (Field field : fields) {
				if (field.isAnnotationPresent(Config.class)) {
					Config confAnnotation = field.getAnnotation(Config.class);
					// FIXME: case conversions
					if (conf.containsKey(confAnnotation.value())) {
						try {
							// setAge
							String methodName = "set" + 
									field.getName().substring(0, 1).toUpperCase() +
									field.getName().substring(1); 						
							Method setter = configuredClass.getMethod(methodName, field.getType());
							setter.invoke(configured, conf.getProperty(confAnnotation.value()));
						} catch (NoSuchMethodException | SecurityException |
								 IllegalAccessException | IllegalArgumentException | 
								 InvocationTargetException e) {
							LOGGER.error("Exception configuring value {} with configuration : {}",
									configured, conf, ExceptionUtils.getFullStackTrace(e));
							throw new RuntimeException(e);
						}
					}
					
//					if (confAnnotation.value().equals(field.getName().toUpperCase())) {
//					}
				}
			}
			
			// configuredClass.getAnnotations();
			
		}
		
		// http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/base/CaseFormat.html
		/*
		public static <V> V loadConf(Configuration conf) {
			return null; // FIXME
		}*/
	}
	

//	public @interface Copyright {
//		   String info() default "";
//		}
}
