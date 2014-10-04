package com.github.juanrh.data42;

import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/*
 * With Lombok @Accessors we are able to chain setters synthetized by Lombok 
 * 
 * http://projectlombok.org/features/experimental/Accessors.html
 */
@ToString @Accessors(chain=true)
public class Person  {
	/*
	 * Cannot use an enum for the arguments of an annotation ("The value 
	 * for annotation attribute Config.value must be a constant expression"), 
	 * and annotations cannot accept concrete enum clases as arguments ("Invalid
	 * type Enum<?> for the annotation attribute Config.value; only primitive type,
	 * String, Class, annotation, enumeration are permitted or 1-dimensional arrays thereof")
	 * 
	 * This "The value for annotation attribute Config.value must be a constant expression" happens
	 * for Conf.AGE even in the following scenario, so something from ConfigKeys.setStringFieldsToFieldName
	 * is even more difficult  
	 * 
	 * public static class Conf {
	 *	public static final String NAME = "NAME";
	 *	public static final String AGE; 
	 *	static {
	 *		AGE = "AGE";
	 *	}
	 * }
	 *  
	 * public static enum Conf {
	 * 	NAME, AGE;
	 * }
	 * */
	/**
	 * Precondition: name convention, UPPER_UNDERSCORE format must be used for the constants and the
	 * name of the constant as value. Also must use lower camel case ("Java variable 
	 * naming convention") for the corresponding members. See http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/base/CaseFormat.html#to%28com.google.common.base.CaseFormat,%20java.lang.String%29
	 *  
	 * TODO: create parent class that makes a check of the constraints, and 
	 * call the check method on construction. This way "Person extends Configured<Person.Conf>"
	 * or "interface Configured<Keys extends Enum<?>>" would have sense for implementing the
	 * check of this contraints
	 * */
	public static class Conf {
		public static final String FIRST_NAME= "FIRST_NAME";
		public static final String AGE = "AGE"; 
	}
	
	// @Setter @Config(Conf.AGE) // same effect as below
	@Setter @Config(key=Conf.AGE)
	private int age;
	
	@Setter @Config(key=Conf.FIRST_NAME)
	private String firstName;
}
