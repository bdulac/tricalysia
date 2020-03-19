package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.github.bdulac.tricalysia.TricalysiaTripleStore;
import io.github.bdulac.tricalysia.spark.SparkTricalysia;

@Disabled
public class LinguisticsTest {
	
	@BeforeAll
	@DisplayName("Extracting semantic knowledge about linguistics in a Spark store.")
	public static void testFill() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:8020/linguistics_extract.txt");
		tri.setAutocommitInterval(1000000);
		tri.negociate(
				"https://www.w3.org/2006/03/wn/wn20/download/wn20basic.zip",
				"http://linguistics-ontology.org/gold-2010.owl", 
				"http://babelnet.org/rdf/"
		);
		tri.close();
	}
	
	@Test
	@DisplayName("Reading a subject in linguistics in the Spark store.")
	public void testRead() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:8020/linguistics_extract.txt");
		String subject = "test";
		Map<String, List<String>> properties = tri.read(subject);
		for(String property : properties.keySet()) {
			List<String> objects = properties.get(property);
			Assertions.assertEquals(1, objects.size());
			String object = objects.get(0);
			String info = String.join(",", subject, property, object);
			Logger.getAnonymousLogger().info(info);
		}
		tri.close();
	}
	
	@Test
	@DisplayName("Reading all subjects in linguistics in a Spark store.")
	public void testSubjects() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		tri.setFilePath("hdfs://localhost:8020/linguistics_extract.txt");
		List<String> subjects = tri.subjects();
		int count = subjects.size();
		Assertions.assertTrue(count > 0);
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
}
