package io.github.bdulac.tricalysia;

import java.util.Map;

/**
 * Interface for a triples store.
 */
public interface Tricalysia {
	
	/**
	 * Loads triples from a set of URLs and register these into a store.
	 * @param store
	 * The store to load the triple into.
	 * @param urls
	 * The URLs to load the triples from.
	 */
	public void negociate(String... urls);

	<S, P, O> void write(S subject, P property, O object);

	<S, P, O> Map<P, O> read(S subject);

	void clear();

}