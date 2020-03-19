package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;

public class JenaRdfExtractorTest {
	
	@Test
	@DisplayName("Extracting the Gold OWL/XML linguistics into a triple store.")
	public void testLoadOwlXml() throws Exception {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.negociate(
					"http://linguistics-ontology.org/gold-2010.owl"
		);
		tri.close();
	}
	
	@Test
	@DisplayName("Extracting an RDF/N3 introduction of the Babel Net dictionary into a triple store.")
	public void testLoadRdfN3BabelNetIntro() throws Exception {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.negociate(
				"http://babelnet.org/rdf/"
		);
		tri.close();
	}
	
	@Test
	@DisplayName("Extracting and OWL/XML introduction of the Babel Net dictionary into a triple store.")
	public void testLoadBabelnet() throws Exception {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.clear();
		tri.negociate("http://babelnet.org/rdf/data/?output=xml");
		String subject = "http://babelnet.org/rdf/data/";
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
	@DisplayName("Extracting Dublin Core and RDF/Turtle term in a triple store.")
	public void testLoadRdfTurle() throws IOException {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.clear();
		tri.negociate("http://purl.org/dc/terms/modified");
		String subject = "http://purl.org/dc/terms/modified";
		Map<String, List<String>> properties = tri.read(subject);
		for(String property : properties.keySet()) {
			List<String> objects = properties.get(property);
			for(String object : objects) {
				String info = String.join(",", subject, property, object);
				Logger.getAnonymousLogger().info(info);
			}
		}
		tri.close();
	}
	
	@Test
	@DisplayName("Extracting data a BNF RDF/XML record into a triple store.")
	public void testLoadRdfXml() throws IOException {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.negociate(
				"http://data.bnf.fr/12000651/jean-baptiste_de_monet_de_lamarck/rdf.xml"
		);
		tri.close();
	}
	
	@Test
	@DisplayName("Extracting a data BFN RDF/N3 record into a triple store.")
	@Disabled
	public void testLoadRdfN3() throws IOException {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.negociate("https://data.bnf.fr/fr/10306982/clement_dulac/rdf.n3");
		Map<String, List<String>> properties = tri.read("https://data.bnf.fr/ark:/12148/cb10306982s");
		Assertions.assertTrue(properties.size() > 0);
		Logger.getAnonymousLogger().info(properties.toString());
		tri.close();
	}
	
	@Test
	@DisplayName("Extracting a data BNF RDF/NT record into a triple store.")
	@Disabled
	public void testLoadRdfNt() throws IOException {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.negociate("http://data.bnf.fr/10306982/clement_dulac/rdf.nt");
		tri.close();
	}
	
	@Test
	@DisplayName("Extracting the Babel Net, Gold and Wordnet dictionaries into a triple store.")
	@Disabled
	public void testLoadMulti() throws Exception {
		Tricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.setAutocommitInterval(100000);
		tri.negociate(
				"https://www.w3.org/2006/03/wn/wn20/download/wn20basic.zip",
				"http://linguistics-ontology.org/gold-2010.owl", 
				"http://babelnet.org/rdf/"
		);
		tri.close();
	}
}