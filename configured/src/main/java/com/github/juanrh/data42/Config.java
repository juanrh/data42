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
	 * entry. Using the default "" implies the name of the field converted to
	 * the usual convention with uppercase and _ will be used as key  
	 */
	 // String value() default "";
	String key(); 
}
