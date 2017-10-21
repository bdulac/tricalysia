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
		try {
			Vertex subj = null;
			GraphTraversal<Vertex, Vertex> s= graph.traversal().V().has(T.label, subject);
			// Iterator<Vertex> s = graph.vertices(subject.hashCode());
			if(s.hasNext()) {
				subj = s.next();
			}
			if(subj == null) {
				subj = graph.addVertex(T.label, subject);
				/*
				Object o = subj.id();
				String l = subj.label();
				boolean ok = o.equals(subject.hashCode());
				boolean ok2 = l.equals(subject);
				*/
			}
			Vertex obj = null;
			// Iterator<Vertex> o = graph.vertices(object.hashCode());
			GraphTraversal<Vertex, Vertex> o = graph.traversal().V().has(T.label, object);
			if(o.hasNext()) {
				obj = o.next();
			}
			if(obj == null) {
				obj = graph.addVertex(T.label, object);
			}
			graph.edges(subj.addEdge(property.toString(), obj));
			tx.commit();
		} catch(Exception e) {
			tx.rollback();
			logger.severe(e.getMessage());
		}
	}
	
	/* (non-Javadoc)
	 * @see io.github.bdulac.tricalysia.Tricalysia#read(S)
	 */
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
	
	/*
	protected void loadFromUrl(String url) 
			throws IOException {
		InputStream in = null;
		try {
			URL u = new URL(url);
			in = u.openStream();
			Model m = ModelFactory.createDefaultModel();
			RDFReader r = m.getReader("RDF/XML");
			r.setProperty("iri-rules", "strict");
			r.setProperty("error-mode", "strict");
			r.read(m, in, url);
			StmtIterator iter = m.listStatements();
			while(iter.hasNext()) {
				Statement st = iter.next();
				Resource subject = st.getSubject();
				Property predicate = st.getPredicate();
				RDFNode object = st.getObject();
				write(subject.toString(), predicate.toString(), object.toString());
			}
		} finally {
			in.close();
		}
	}
	*/

	/* (non-Javadoc)
	 * @see io.github.bdulac.tricalysia.Tricalysia#clear()
	 */
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
