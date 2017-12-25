package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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
public class JenaRdfExtractor implements TriplesExtractor {
	
	public JenaRdfExtractor() {
	}
	
	@Override
	public List<String> getSupportedMimeTypes() {
		List<String> result = new ArrayList<String>();
		result.add("application/rdf+xml");
		return result;
	}

	@Override
	public List<URL> extract(Tricalysia store, URL url) throws IOException {
		if(store == null) {
			throw new IllegalArgumentException();
		}
		List<URL> result = new ArrayList<URL>();
		if(url == null)return result;
		InputStream in = null;
		try {
			in = url.openStream();
			Model m = ModelFactory.createDefaultModel();
			RDFReader r = m.getReader("RDF/XML");
			r.setProperty("iri-rules", "strict");
			r.setProperty("error-mode", "strict");
			r.read(m, in, url.toString());
			StmtIterator iter = m.listStatements();
			while(iter.hasNext()) {
				Statement st = iter.next();
				Resource subject = st.getSubject();
				fetchRelatedURLFromUriString(subject.getURI(), url, result);
				Property predicate = st.getPredicate();
				fetchRelatedURLFromUriString(predicate.getURI(), url, result);
				RDFNode object = st.getObject();
				fetchRelatedURLFromUriString(object.toString(), url, result);
				store.write(
						subject.toString(), 
						predicate.toString(), 
						object.toString()
				);
			}
		} finally {
			in.close();
		}
		return result;
	}
	
	private void fetchRelatedURLFromUriString(
			String str, URL refUrl, List<URL> result
	) {
		if(
				(str != null) 
				&& (!str.equals(refUrl.toString()))
				&& (str.startsWith("http://"))
		) {
			try {
				URL u = new URL(str);
				if(!result.contains(u)) {
					result.add(u);
				}
			} catch (MalformedURLException e) {
				return;
			}
		}
		else return;
	}
}