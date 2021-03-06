package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

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
	
	private static final Logger logger = 
			Logger.getLogger(JenaRdfExtractor.class.getName());
	
	public JenaRdfExtractor() {
	}
	
	@Override
	public List<String> getSupportedMimeTypes() {
		List<String> result = new ArrayList<String>();
		result.add("application/rdf+xml");
		result.add("application/xml");
		result.add("text/turtle");
		result.add("text/n3");
		result.add("text/rdf+n3");
		result.add("text/plain");
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
			URLConnection ct = url.openConnection();
			if(ct == null) {
				logger.severe("Connection to URL " + url + " failed");
				return result;
			}
			String contentType = ct.getContentType();
			if(contentType == null) {
				contentType = "application/rdf+xml";
			}
			if(contentType.contains(";")) {
				contentType = 
						contentType.substring(0, contentType.indexOf(";"));
			}
			in = ct.getInputStream();
			Model m = ModelFactory.createDefaultModel();
			RDFReader r = null;
			if("application/rdf+xml".equals(contentType)) {
				r = m.getReader("RDF/XML");
			}
			else if("application/xml".equals(contentType)) {
				r = m.getReader("RDF/XML");
			}
			else if("text/turtle".equals(contentType)) {
				r = m.getReader("Turtle");
			}
			else if("text/n3".equals(contentType)) {
				r = m.getReader("N3");
			}
			else if("text/rdf+n3".equals(contentType)) {
				r = m.getReader("N3");
			}
			else if("text/plain".equals(contentType)) {
				r = m.getReader("NT");
			}			
			r.setProperty("iri-rules", "strict");
			r.setProperty("error-mode", "strict");
			r.read(m, in, url.toExternalForm());
			StmtIterator iter = m.listStatements();
			while(iter.hasNext()) {
				Statement st = iter.next();
				Resource subject = st.getSubject();
				String s = subject.getURI();
				if(s == null) {
					s = subject.toString();
				}
				fetchRelatedURLFromUriString(s, url, result);
				Property predicate = st.getPredicate();
				String p = predicate.getURI();
				if(p == null) {
					p = predicate.toString();
				}
				fetchRelatedURLFromUriString(p, url, result);
				RDFNode object = st.getObject();
				fetchRelatedURLFromUriString(object.toString(), url, result);
				store.write(
						s, 
						p, 
						object.toString()
				);
			}
		} catch(IOException e) {
			e.printStackTrace();
			
			logger.warning(
					"An error occurred while loading the content from URL " 
					+ url + ":" + e.getMessage()
			);
		} finally {
			if(in != null) {
				in.close();
			}
		}
		return result;
	}
	
	private void fetchRelatedURLFromUriString(
			String str, URL refUrl, List<URL> result
	) {
		try {
			// Only absolute URLs are considerœed
			if(str.startsWith("http")) {
				URL u = URLResolver.resolveRelativeURL(str, refUrl);
				if((!result.contains(u)) && (!u.equals(refUrl))) {
					result.add(u);
				}
				else return;
			}
		} catch (MalformedURLException e) {
			Logger.getAnonymousLogger().warning(
					e.getMessage() + " (URL=" + str + ")"
			);
			return;
		}
	}
}