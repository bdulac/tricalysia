package io.github.bdulac.tricalysia.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Ignore;
import org.junit.Test;

import io.github.bdulac.tricalysia.TricalysiaTestUtils;
import junit.framework.Assert;

@Ignore
public class LinguisticsTest {
	
	@Test
	@Ignore
	public void testRead() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:9000/linguistics.txt");
		String subject = "test";
		Map<String, List<String>> properties = tri.read(subject);
		for(String property : properties.keySet()) {
			List<String> objects = properties.get(property);
			Assert.assertEquals(1, objects.size());
			String object = objects.get(0);
			String info = String.join(",", subject, property, object);
			Logger.getAnonymousLogger().info(info);
		}
		tri.close();
	}
	
	@Test
	@Ignore
	public void testSubjects() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:9000/linguistics.txt");
		List<String> subjects = tri.subjects();
		int count = subjects.size();
		Assert.assertTrue(count > 0);
		Logger.getAnonymousLogger().info("" + count + " triples");
		for(String subject : subjects) {
			Map<String, List<String>> properties = tri.read(subject);
			for(String property : properties.keySet()) {
				List<String> objects = properties.get(property);
				for(String object : objects) {
					String info = String.join(",", subject, property, object);
					Logger.getAnonymousLogger().info(info);
				}
			}
		}
		tri.close();
	}
	
	@Test
	@Ignore
	public void testLoadBabelnet() throws Exception {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:9000/babelnet.txt");
		tri.setAutocommitInterval(100000);
		tri.clear();
		tri.negociate("http://babelnet.org/rdf/data/?output=xml");
		List<String> subjects = tri.subjects();
		int count = subjects.size();
		Assert.assertTrue(count > 0);
		Logger.getAnonymousLogger().info("" + count + " triples");
		String subject = "http://babelnet.org/rdf/data/";
		Map<String, List<String>> properties = tri.read(subject);
		for(String property : properties.keySet()) {
			List<String> objects = properties.get(property);
			Assert.assertEquals(1, objects.size());
			String object = objects.get(0);
			String info = String.join(",", subject, property, object);
			Logger.getAnonymousLogger().info(info);
		}
		tri.close();
	}
	
	@Test
	@Ignore
	public void testLoadMulti() throws Exception {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:9000/linguistics.txt");
		tri.setAutocommitInterval(100000);
		tri.negociate(
				"https://www.w3.org/2006/03/wn/wn20/download/wn20basic.zip",
				"http://linguistics-ontology.org/gold-2010.owl", 
				"http://babelnet.org/rdf/"
		);
		tri.close();
	}

}
