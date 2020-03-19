package io.github.bdulac.tricalysia.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import io.github.bdulac.tricalysia.hdfs.AbstractHdfsTricalysia;

/** 
 * Implementation of the triples store using a single HDFS file, taking 
 * advantage of <em>Apache Spark</em>.
 */
public class SparkTricalysia extends AbstractHdfsTricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(SparkTricalysia.class.getName());
	
	/** <em>Apache Spark</em> context. */
	private JavaSparkContext sparkContext;
	
	/**
	 * Construction of a triples store using HDFS using the default 
	 * HDFS configuration.
	 * @param sc
	 * <em>Apache Spark</em> context.
	 * @param fPath
	 * HDFS store file path.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	public SparkTricalysia(JavaSparkContext sc, String fPath) 
			throws IOException, URISyntaxException {
		this(sc, fPath, null);
	}
	
	/**
	 * Construction of a triples store using HDFS using a specific 
	 * HDFS configuration.
	 * @param sc
	 * <em>Apache Spark</em> context.
	 * @param fPath
	 * HDFS store file path.
	 * @param hConf
	 * HDFS configuration.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	public SparkTricalysia(
			JavaSparkContext sc, 
			String fPath, 
			Configuration hConf
	) throws IOException, URISyntaxException {
		super(fPath, hConf);
		if(sc == null) {
			throw new IllegalArgumentException();
		}
		sparkContext = sc;
	}
	
	@Override
	public <N> boolean exists(N node) {
		String n = toObjectString(node);
		JavaRDD<String> fileRdd = sparkContext.textFile(getFilePath())
				.filter(new TripleFilter(n));
		return !fileRdd.isEmpty();
	}
	
	@Override
	public <S, P, O> boolean exists(S subject, P property, O object) {
		String s = toSubjectString(subject);
		String p = toSubjectString(property);
		String o = toSubjectString(object);
		JavaRDD<String> fileRdd = sparkContext.textFile(getFilePath())
				.filter(new TripleFilter(s, p, o));
		return !fileRdd.isEmpty();
	}
	
	@SuppressWarnings("unchecked")
	public <S> List<S> subjects() {
		JavaRDD<String> rddLines = sparkContext.textFile(getFilePath());
		List<String> triplesLines = rddLines.collect();
		List<S> subjects = new ArrayList<S>();
		for(String tripleLine : triplesLines) {
			String[] triple = TripleFilter.parseRfc4180Line(tripleLine);
			if(triple.length != 3) {
				logger.warning("Illegal triple: " + triple);
			}
			else {
				String subject = triple[0];
				subjects.add((S)subject);
				/*
				String b = triple[1];
				String c = triple[2];
				*/
			}
		}
		return subjects;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S, P, O> Map<P, List<O>> read(S subject) {
		String s = toObjectString(subject);
		Map<P, List<O>> properties = new HashMap<P, List<O>>();
		JavaRDD<String> rddLines = sparkContext.textFile(getFilePath());
		Function<String, Boolean> filter = new TripleFilter(s);
		JavaRDD<String> subjectLines = 
				rddLines.filter(filter);
		List<String> triplesLines = subjectLines.collect();
		for(String tripleLine : triplesLines) {
			String[] triple = TripleFilter.parseRfc4180Line(tripleLine);
			if(triple.length != 3) {
				logger.warning("Illegal triple: " + String.join(",", triple));
			}
			else {
				String a = triple[0];
				String b = triple[1];
				String c = triple[2];
				List<O> objects = null;
				O o = null;
				P key = null;
				if(s.equals(a)) {
					key = (P)b;
					objects = properties.get(key);
					if(objects == null) {
						objects = new ArrayList<O>();
					}
					o = (O)c;
				}
				else if(s.equals(b)) {
					key = (P)a;
					objects = properties.get(key);
					if(objects == null) {
						objects = new ArrayList<O>();
					}
					o = (O)c;
				}
				else if(s.equals(c)) {
					key = (P)a;
					objects = properties.get(key);
					if(objects == null) {
						objects = new ArrayList<O>();
					}
					o = (O)b;
				}
				if(!objects.contains(o)) {
					objects.add(o);
				}
				properties.put(key, objects);
			}
		}
		return properties;
	}
}