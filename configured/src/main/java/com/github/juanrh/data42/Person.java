package com.github.juanrh.data42;

import lombok.Setter;
import lombok.ToString;

@ToString
public class Person implements Configured<Person.Conf>{
	public static enum Conf {
		NAME, AGE;
	}
	
	@Setter 
	private String name;
	@Setter @Config("AGE")
	private int age;  
}
