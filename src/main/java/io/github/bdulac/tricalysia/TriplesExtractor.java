package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Extract triples from a remote URL and loads these in a triples store.
 */
public interface TriplesExtractor {
	
	public List<String> getSupportedMimeTypes();
	
	/**
	 * Loads triples from an URL and register these into a store.
	 * @param url
	 * The URL to load the triples from.
	 * @param store
	 * The store to load the triple into.
	 * @return List of extracted URLs in the triples analysis.
	 * @throws IOException
	 * If the transfer from the URL fails.
	 */
	List<URL> extract(Tricalysia store, URL url) throws IOException;
}
