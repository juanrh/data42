package com.github.juanrh.data42;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

import com.google.common.collect.ImmutableMap;

public class Application {
	public static void main(String [] args) {
		Person person = new Person();
		System.out.println(person.toString());
		
		person.setAge(33);
		person.setName("Juan");
		System.out.println(person.toString());
		
		Configuration conf = new MapConfiguration(ImmutableMap.of("AGE", 32));
		Configured.Utils.loadConf(conf, person);
		System.out.println(person.toString());
	}
}
