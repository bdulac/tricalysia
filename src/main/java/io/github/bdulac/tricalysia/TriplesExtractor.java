package io.github.bdulac.tricalysia;

import java.io.IOException;

/**
 * Extract triples from a remote URL and loads these in a triples store.
 */
public interface TriplesExtractor {
	
	/**
	 * Loads triples from a set of URLs and register these into a store.
	 * @param store
	 * The store to load the triple into.
	 * @param urls
	 * The URLs to load the triples from.
	 */
	void extract(Tricalysia store, String... urls);
	
	/**
	 * Loads triples from an URL and register these into a store.
	 * @param url
	 * The URL to load the triples from.
	 * @param store
	 * The store to load the triple into.
	 * @throws IOException
	 * If the transfer from the URL fails.
	 */
	void extract(Tricalysia store, String url) throws IOException;
}
