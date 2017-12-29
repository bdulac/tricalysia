package io.github.bdulac.tricalysia.gremlin;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import io.github.bdulac.tricalysia.AbstractTricalysia;

public class GremlinTricalysia extends AbstractTricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractTricalysia.class.getName());
	
	private Graph graph;
	
	public GremlinTricalysia(Graph g) {
		super();
		if(g == null) {
			throw new IllegalArgumentException();
		}
		graph = g;
	}
	
	@Override
	public <S,P,O> void write(S subject, P property, O object) {
		Transaction tx = graph.tx();
		if(subject == null) {
			logger.warning(
					"Subject can not be empty [triple=" 
					+ subject + ";" 
					+ property + ";"
					+ object + "]" 
			);
			return;
		}
		if(property == null) {
			logger.warning(
					"Property can not be empty [triple=" 
					+ subject + ";" 
					+ property + ";"
					+ object + "]" 
			);
			return;
		}
		if(object == null) {
			logger.warning(
					"Object can not be empty [triple=" 
					+ subject + ";" 
					+ property + ";"
					+ object + "]" 
			);
			return;
		}
		try {
			Vertex subj = null;
			GraphTraversal<Vertex, Vertex> s = 
					graph.traversal().V().has(T.label, toSubjectString(subject));
			if(s.hasNext()) {
				subj = s.next();
			}
			if(subj == null) {
				subj = graph.addVertex(T.label, toSubjectString(subject));
			}
			Vertex obj = null;
			GraphTraversal<Vertex, Vertex> o = 
					graph.traversal().V().has(T.label, toObjectString(object));
			if(o.hasNext()) {
				obj = o.next();
			}
			if(obj == null) {
				obj = graph.addVertex(T.label, toObjectString(object));
			}
			Edge e = null;
			Iterator<Edge> edges = 
					subj.edges(Direction.OUT, toPropertyString(property));
			if(edges.hasNext()) {
				e = edges.next();
			}
			if(e == null) {
				e = subj.addEdge(toPropertyString(property), obj);
				graph.edges(e);
			}
			tx.commit();
		} catch(Exception e) {
			tx.rollback();
			logger.severe(e.getMessage());
		}
	}
	
	private <S> String toSubjectString(S subject) {
		return subject.toString();
	}
	
	private <P> String toPropertyString(P property) {
		String prop = property.toString();
		if(prop.startsWith("http://www.w3.org")) {
			return prop.substring(prop.lastIndexOf('/'));
		}
		else if(prop.contains("/dc/terms/")) {
			return prop.substring(prop.indexOf("/dc/"));
		}
		return prop;
	}
	
	private <O> String toObjectString(O object) {
		return object.toString();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S, P, O> Map<P, O> read(S subject) {
		Iterator<Vertex> vertices = graph.vertices();
		Vertex v = null;
		Map<P, O> result = new HashMap<P, O>();
		while(vertices.hasNext()) {
			v = vertices.next();
			String lb = v.label();
			if(lb.equals(subject)) {
				Iterator<Edge> properties = v.edges(Direction.BOTH);
				while(properties.hasNext()) {
					Edge p = properties.next();
					String property = p.label();
					Iterator<Vertex> verts = p.bothVertices();
					while(verts.hasNext()) {
						Vertex v2 = verts.next();
						if(!(v2.label().equals(v.label()))) {
							String object = v2.label();
							result.put((P)property, (O)object);
						}
					}
				}
			}
		}
		return result;
	}

	@Override
	public void clear() {
		Transaction tx = graph.tx();
		Iterator<Vertex> vertices = graph.vertices();
		while(vertices.hasNext()) {
			Vertex v = vertices.next();
			v.remove();
		}
		Iterator<Edge> edges = graph.edges();
		while(edges.hasNext()) {
			Edge e = edges.next();
			e.remove();
		}
		tx.commit();
	}
}