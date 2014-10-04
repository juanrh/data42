package com.github.juanrh.data42;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
					LOGGER.error("Exception setting fields to ");
					e.printStackTrace();
				}	
			}
		}
	}
}
