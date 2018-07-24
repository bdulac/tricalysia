package io.github.bdulac.tricalysia.gremlin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

/**
 * Implementation of the triples store interface taking advantage of 
 * <em>Apache Tinkerpop</em> and its <em>Gremlin</em> graph traversal 
 * library.
 */
public class GremlinTricalysia extends AbstractTricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractTricalysia.class.getName());
	
	protected Graph graph;
	
	protected Transaction tx;
	
	public GremlinTricalysia(Graph g) {
		super();
		if(g == null) {
			throw new IllegalArgumentException();
		}
		graph = g;
	}
	
	@Override
	protected <S> String toSubjectString(S subject) {
		String res = super.toObjectString(subject);
		res = res.replace("`", "``");
		return res;
	}
	
	@Override
	protected <P> String toPropertyString(P property) {
		String prop = super.toPropertyString(property);
		prop = prop.replace("`", "``");
		return prop;
	}
	
	@Override
	protected <O> String toObjectString(O object) {
		String res = super.toObjectString(object);
		res = res.replace("`", "``");
		return res;
	}
	
	@Override
	public void startTransaction() {
		tx = graph.tx();
	}
	
	@Override
	public void commitTransaction() {
		tx.commit();
		tx.close();
	}
	
	@Override
	public void rollbackTransaction() {
		if(tx.isOpen()){
			tx.rollback();
		}
		tx.close();
	}
	
	@Override
	public <S,P,O> void write(S subject, P property, O object) {
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
		startAbstractTransaction();
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
			commitAbstractTransaction();
		} catch(RuntimeException e) {
			rollbackTransaction();
			logger.severe(e.getMessage());
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <S, P, O> Map<P, List<O>> read(S subject) {
		Vertex subj = null;
		Map<P, List<O>> result = new HashMap<P, List<O>>();
		GraphTraversal<Vertex, Vertex> s = 
				graph.traversal().V().has(T.label, toSubjectString(subject));
		if(s.hasNext()) {
			subj = s.next();
			Iterator<Edge> properties = subj.edges(Direction.BOTH);
			while(properties.hasNext()) {
				Edge p = properties.next();
				String property = p.label();
				Iterator<Vertex> verts = p.bothVertices();
				while(verts.hasNext()) {
					Vertex v2 = verts.next();
					if(!(v2.label().equals(subj.label()))) {
						String object = v2.label();
						List<O> objects = null;
						P key = (P)property;
						objects = result.get(key);
						if(objects == null) {
							objects = new ArrayList<O>();
						}
						objects.add((O)object);
						result.put(key, objects);
					}
				}
			}
		}
		return result;
	}
	
	public Graph getGraph() {
		return graph;
	}
	
	@Override
	public <S> boolean exists(S subject) {
		boolean result = false;
		GraphTraversal<Vertex, Vertex> s = 
				graph.traversal().V().has(T.label, toSubjectString(subject));
		result = (s.hasNext());
		if(!result) {
			GraphTraversal<Edge, Edge> e = 
					graph.traversal().E().has(T.label, toSubjectString(subject));
			result = e.hasNext();
		}
		return result;
	}
	
	public <S, P, O> boolean exists(S subject, P property, O object) {
		boolean result = false;
		GraphTraversal<Vertex, Vertex> p = 
				graph.traversal().V().has(T.label, toPropertyString(property));
		result = (p.hasNext());
		if(result) {
			GraphTraversal<Edge, Edge> s = 
					graph.traversal().E()
					.has(T.label, toSubjectString(subject));
			result = s.hasNext();
			if(result) {
				GraphTraversal<Edge, Edge> o = 
						graph.traversal().E()
						.has(T.label, toObjectString(object));
				result = o.hasNext();
			}
		}
		return result;
	}
	
	@Override
	public void close() throws IOException {
		try {
			graph.close();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void clear() {
		startTransaction();
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
		commitTransaction();
	}
}