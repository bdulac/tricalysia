package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;
import io.github.bdulac.tricalysia.spark.SparkTricalysia;

/**
 * Testing elements. Triples stores setup.
 */
public class TricalysiaTestUtils {
	
	private static final SparkConf conf;
	
	private static JavaSparkContext context;
	
	static {
		conf = new SparkConf().setMaster("local").setAppName("graph");
		
	}

	@SuppressWarnings("unchecked")
	public static <T extends Tricalysia> T setupTestStore(Class<T> cl) {
		T result = null;
		if(cl.equals(GremlinTricalysia.class)) {
			Graph graph = Neo4jGraph.open("/tmp/neo4j");
			result = (T)new GremlinTricalysia(graph);
				
		}
		else if(cl.equals(SparkTricalysia.class)) {
			String fPath = "hdfs://localhost:54310/test.file";
			try {
				if(context == null) {
					context = new JavaSparkContext(conf);
				}
				else {
					context.close();
					context = new JavaSparkContext(conf);
				}
				result = (T)new SparkTricalysia(context, fPath);
			} catch (IOException | URISyntaxException e) {
				throw new IllegalStateException(e);
			}
			
		}
		else {
			throw new IllegalArgumentException(cl.getName());
		}
		return result;
	}

}
