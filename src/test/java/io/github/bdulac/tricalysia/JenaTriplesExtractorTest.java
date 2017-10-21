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

public class JenaTriplesExtractorTest extends TestCase {
	
	public void testLoadRdf() throws Exception {
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
			tri.getUrlExtractor().extract(
					tri, 
					"https://science.mnhn.fr/institution/mnhn/collection/ed/item/ed4815.rdf",
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j16268.rdf",
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j03230.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j04906.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j03231.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j03234.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j03235.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/r05178.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/r05175.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/r05177.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/r05173.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/r05176.rdf", 
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j03236.rdf"
			);
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(graph.edges()) + " edges");
		} finally {
			graph.close();
		}
		
	}
}