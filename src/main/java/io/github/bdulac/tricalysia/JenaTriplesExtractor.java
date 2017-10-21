package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.RDFReader;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

/**
 * Triples extraction using Apache Jena.
 */
public class JenaTriplesExtractor extends AbstractTriplesExtractor {
	
	public JenaTriplesExtractor() {
	}

	@Override
	public void extract(Tricalysia store, String url) throws IOException {
		if(url == null)return;
		if(store == null) {
			throw new IllegalArgumentException();
		}
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
				store.write(
						subject.toString(), 
						predicate.toString(), 
						object.toString()
				);
			}
		} finally {
			in.close();
		}
	}
}