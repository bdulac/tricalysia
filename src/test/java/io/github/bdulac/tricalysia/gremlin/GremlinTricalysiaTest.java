package io.github.bdulac.tricalysia.gremlin;

import java.util.Map;

//import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;
import junit.framework.Assert;
import junit.framework.TestCase;

public class GremlinTricalysiaTest extends TestCase {
	
	public void testTriple() {
		Graph graph = Neo4jGraph.open("/tmp/neo4j");
		GremlinTricalysia tri = new GremlinTricalysia(graph);
		tri.clear();
		tri.write("my subject", "my property", "my object");
		Assert.assertEquals(2, IteratorUtils.count(graph.vertices()));
		Assert.assertEquals(1, IteratorUtils.count(graph.edges()));
		Map<Object, Object> result = tri.read("my subject");
		Assert.assertEquals(1, result.size());
		Assert.assertEquals("my property", result.keySet().iterator().next());
		Assert.assertEquals("my object", result.values().iterator().next());
	}
}