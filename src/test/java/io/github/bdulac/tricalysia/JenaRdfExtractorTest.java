package io.github.bdulac.tricalysia;

import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;
import junit.framework.TestCase;

public class JenaRdfExtractorTest extends TestCase {
	
	public void testLoadRdfXml() throws Exception {
		Driver driver = 
				 GraphDatabase.driver(
						 "bolt://localhost:7687", 
						 AuthTokens.basic("neo4j", "neo4j")
				);
		Neo4JElementIdProvider<?> vertexProvider = new Neo4JNativeElementIdProvider();
		Neo4JElementIdProvider<?> edgeProvider = new Neo4JNativeElementIdProvider();
		Graph graph = null;
		try {
			graph = new Neo4JGraph(driver, vertexProvider, edgeProvider);
			graph.features().vertex().supportsNumericIds();
			graph.features().edge().supportsCustomIds();
			Tricalysia tri = new GremlinTricalysia(graph);
			tri.clear();
			tri.negociate(
					"http://data.bnf.fr/12000651/jean-baptiste_de_monet_de_lamarck/rdf.xml"
			);
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.edges()) + " edges");
		} finally {
			graph.close();
		}
	}
	
	public void testLoadRdfTurle() throws Exception {
		Driver driver = 
				 GraphDatabase.driver(
						 "bolt://localhost:7687", 
						 AuthTokens.basic("neo4j", "neo4j")
				);
		Neo4JElementIdProvider<?> vertexProvider = new Neo4JNativeElementIdProvider();
		Neo4JElementIdProvider<?> edgeProvider = new Neo4JNativeElementIdProvider();
		Graph graph = null;
		try {
			graph = new Neo4JGraph(driver, vertexProvider, edgeProvider);
			graph.features().vertex().supportsNumericIds();
			graph.features().edge().supportsCustomIds();
			Tricalysia tri = new GremlinTricalysia(graph);
			tri.clear();
			tri.negociate("http://purl.org/dc/terms/modified");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.edges()) + " edges");
		} finally {
			graph.close();
		}
	}
	
	public void testLoadRdfN3() throws Exception {
		Driver driver = 
				 GraphDatabase.driver(
						 "bolt://localhost:7687", 
						 AuthTokens.basic("neo4j", "neo4j")
				);
		Neo4JElementIdProvider<?> vertexProvider = new Neo4JNativeElementIdProvider();
		Neo4JElementIdProvider<?> edgeProvider = new Neo4JNativeElementIdProvider();
		Graph graph = null;
		try {
			graph = new Neo4JGraph(driver, vertexProvider, edgeProvider);
			graph.features().vertex().supportsNumericIds();
			graph.features().edge().supportsCustomIds();
			Tricalysia tri = new GremlinTricalysia(graph);
			tri.clear();
			tri.negociate("http://wikidata.dbpedia.org/data/Q82122.n3");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.edges()) + " edges");
		} finally {
			graph.close();
		}
	}
	
	public void testLoadRdfNt() throws Exception {
		Driver driver = 
				 GraphDatabase.driver(
						 "bolt://localhost:7687", 
						 AuthTokens.basic("neo4j", "neo4j")
				);
		Neo4JElementIdProvider<?> vertexProvider = new Neo4JNativeElementIdProvider();
		Neo4JElementIdProvider<?> edgeProvider = new Neo4JNativeElementIdProvider();
		Graph graph = null;
		try {
			graph = new Neo4JGraph(driver, vertexProvider, edgeProvider);
			graph.features().vertex().supportsNumericIds();
			graph.features().edge().supportsCustomIds();
			Tricalysia tri = new GremlinTricalysia(graph);
			tri.clear();
			tri.negociate("http://data.bnf.fr/10306982/clement_dulac/rdf.n3");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.edges()) + " edges");
		} finally {
			graph.close();
		}
	}
}
