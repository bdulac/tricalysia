package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Assert;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;
import io.github.bdulac.tricalysia.spark.SparkTricalysia;
import junit.framework.TestCase;

public class JenaRdfExtractorTest extends TestCase {
	
	public void testLoadRdfXml() throws IOException {
		Tricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		tri.negociate(
				"http://data.bnf.fr/12000651/jean-baptiste_de_monet_de_lamarck/rdf.xml"
		);
		tri.close();
	}
	
	public void testLoadOwlXml() throws Exception {
		Tricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		tri.negociate(
					"http://linguistics-ontology.org/gold-2010.owl"
		);
		tri.close();
	}
	
	public void testLoadRdfN3Gremlin() throws Exception {
		Tricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		tri.negociate(
				"http://babelnet.org/rdf/"
		);
		tri.close();
	}
	
	public void testLoadRdfTurle() throws IOException {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		tri.negociate("http://purl.org/dc/terms/modified");
		List<String> subjects = tri.subjects();
		Assert.assertTrue(subjects.size() > 0);
		Logger.getAnonymousLogger().info("" + subjects + " triples");
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
	
	public void testLoadRdfN3Spark() throws IOException {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		tri.negociate("http://dbpedia.org/data/Taxon.n3");
		List<String> subjects = tri.subjects();
		Assert.assertTrue(subjects.size() > 0);
		Logger.getAnonymousLogger().info("" + subjects + " triples");
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
		Map<String, List<String>> properties = 
				tri.read("http://dbpedia.org/resource/Taxon");
		Assert.assertTrue(properties.size() > 0);
		Logger.getAnonymousLogger().info(properties.toString());
		tri.close();
	}
	
	public void testLoadRdfNt() throws IOException {
		Tricalysia tri = 
				TricalysiaTestUtils.setupTestStore(GremlinTricalysia.class);
		tri.clear();
		tri.negociate("http://data.bnf.fr/10306982/clement_dulac/rdf.nt");
		tri.close();
	}
}
