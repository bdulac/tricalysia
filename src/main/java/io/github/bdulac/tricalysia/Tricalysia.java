package io.github.bdulac.tricalysia;

import java.util.Map;

/**
 * Interface for a triples store.
 */
public interface Tricalysia {
	
	TriplesExtractor getUrlExtractor();

	<S, P, O> void write(S subject, P property, O object);

	<S, P, O> Map<P, O> read(S subject);

	void clear();

}