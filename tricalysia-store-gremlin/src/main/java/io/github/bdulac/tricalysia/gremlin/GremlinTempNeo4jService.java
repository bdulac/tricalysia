package io.github.bdulac.tricalysia.gremlin;

import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import io.github.bdulac.tricalysia.TripleStoreService;

public class GremlinTempNeo4jService implements TripleStoreService<GremlinTricalysia> {

	@Override
	public Class<GremlinTricalysia> getSupportedClass() {
		return GremlinTricalysia.class;
	}
	
	@Override
	public GremlinTricalysia setupTripleStore(Class<GremlinTricalysia> cl) {
		Graph graph = Neo4jGraph.open("/tmp/neo4j");
		GremlinTricalysia result = new GremlinTricalysia(graph);
		return result;
	}
	

}
