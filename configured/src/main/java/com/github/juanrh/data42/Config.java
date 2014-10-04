package com.github.juanrh.data42;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Config {
	/**
	 * String corresponding to a org.apache.commons.configuration.Configuration 
	 * entry. No default is allowed in order to get a more explicit code that
	 * doesn't rely on naming conventions
	 */
	String key(); 
}
