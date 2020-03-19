 package io.github.bdulac.tricalysia.spark;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

public class TripleFilter implements Function<String, Boolean>, Serializable {
	
	/** @see java.io.Serializable */
	private static final long serialVersionUID = -1255803437012345187L;
	
	private String subject;
	
	private String property;
	
	private String object;
	
	public TripleFilter(String s) {
		subject = s;
	}
	
	public TripleFilter(String s, String p, String o) {
		this(s);
		property = p;
		object = o;
	}
	
	
	@Override
	public Boolean call(String t) {
		String[] values = parseRfc4180Line(t);
		boolean result = false;
		if(property != null && object != null) {
			result = equals(subject, property, object, values);
		}
		else {
			result = contains(subject, values);
		}
		return result;
	}
	
	public static String[] parseRfc4180Line(String line) {
		String[] tokens = line.split("\",\"");
		String[] result = new String[tokens.length];
		int index = 0;
		for(String token : tokens) {
			String val = token;
			if(val.startsWith("\"")) {
				val = val.substring(1);
			}
			if(val.endsWith("\"")) {
				val = val.substring(0, val.length() - 1);
			}
			val = val.replaceAll("\"\"", "\"");
			// FIXME manage language indication...
			// e.g. @en-US
			result[index] = val;
			index++;
		}
		return result;
	}
	
	private boolean equals(String s, String p, String o, String... values) {
		String sCompare = values[0];
		String pCompare = values[1];
		String oCompare = values[2];
		boolean result = equals(subject, sCompare);
		result = result && equals(property, pCompare);
		result = result && equals(object, oCompare);
		return result;
	}
	
	private boolean equals(String a, String b) {
		if(a == null) {
			return b == null;
		}
		else {
			return a.equals(b);
		}
	}
	
	private boolean contains(String key, String... values) {
		for(String val : values) {
			if(key == null) {
				if(val == null) {
					return true;
				}
			}
			if(key.equals(val)) {
				return true;
			}
			
		}
		return false;
	}
}
