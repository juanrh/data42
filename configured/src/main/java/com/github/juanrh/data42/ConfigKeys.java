package com.github.juanrh.data42;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ConfigKeys {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigKeys.class); 

	protected ConfigKeys() {
		this.setStringFieldsToFieldName();
	}
	
	protected void setStringFieldsToFieldName() {
		Field[] fields = this.getClass().getDeclaredFields();
		for (Field field : fields) {
			if (field.getType().isAssignableFrom(String.class)) {
				try {
					field.set(this, field.getName());
				} catch (IllegalArgumentException | IllegalAccessException e) {
					LOGGER.error("Exception setting fields to the field name: {}", ExceptionUtils.getFullStackTrace(e));
					throw new RuntimeException(e); 
				}	
			}
		}
	}
	
	/**
	 * Checks that all the public static final String fields of configKeysClass 
	 * have its name as value
	 * @throws IllegalStateException if the check is not passed
	 * */
	protected static void checkKeyValues(Class<?> configKeysClass) {
		Field [] fields = configKeysClass.getDeclaredFields(); 
		for (Field field : fields) {
			if (Modifier.isPublic(field.getModifiers()) &&
				Modifier.isStatic(field.getModifiers()) &&  
			    Modifier.isFinal(field.getModifiers()) &&	
				field.getType().isAssignableFrom(String.class)) {
				try {
					Preconditions.checkState(
							field.get(null).toString().equals(field.getName()),
							"value for field %s should be %s instead of %s",
							field.getName(), field.getName(), field.get(null).toString()  
							);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					LOGGER.error("Exception checking the values of the public static final "
							+ "String fields of class {}: {}", configKeysClass.getName(), 
							ExceptionUtils.getFullStackTrace(e));
					throw new RuntimeException(e);
				}
			}
		}		 
	} 
}
