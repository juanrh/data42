package com.github.juanrh.data42;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.AbstractConfiguration;

import com.google.auto.value.AutoValue;

public class CheckedConfig extends AbstractConfiguration {
	private Map<String, Object> map;
	
	@AutoValue
	static abstract class TypedKey {
		TypedKey() {}
		
		public static TypedKey create(String key, @SuppressWarnings("rawtypes") Class type) {
			return new AutoValue_CheckedConfig_TypedKey(key, type); 
		}
		
		public abstract String key();  
		@SuppressWarnings("rawtypes")
		public abstract Class type(); 
	}
		
	public CheckedConfig() {
		this.map = new HashMap<>();
	}
	
	@Override
	public boolean isEmpty() {
		return map.isEmpty(); 
	}

	@Override
	public boolean containsKey(String key) {
		return map.containsKey(key); 
	}

	@Override
	public Object getProperty(String key) {
		return map.get(key);
	}

	@Override
	public Iterator<String> getKeys() {
		return map.keySet().iterator(); 
	}

	@Override
	protected void addPropertyDirect(String key, Object value) {
		map.put(key, value);
	} 
}
