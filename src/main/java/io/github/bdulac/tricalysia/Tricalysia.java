package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Simple interface for a triples store. 
 */
public interface Tricalysia {
	
	/**
	 * Loads triples from a set of URLs and register these into the triples 
	 * store.
	 * @param urls
	 * The URLs to load the triples from.
	 */
	public void negociate(String... urls);
	
	/**
	 * Checks if the triple store contains a node.
	 * @param node
	 * The node.
	 * @param <N>
	 * Type of the node.
	 * @return {@code true} if the store contains the specified node.
	 */
	<N> boolean exists(N node);
	
	/**
	 * Checks if the triple store contains a triple.
	 * @param subject
	 * The subject of the triple.
	 * @param property
	 * The property of the triple.
	 * @param object
	 * The object of the triple.
	 * @param <S>
	 * Type of the subject of the triple.
	 * @param <P>
	 * Type of the property of the triple.
	 * @param <O>
	 * Type of the object of the triple.
	 * @return {@code true} if the store contains the specified triple.
	 */
	<S, P, O> boolean exists(S subject, P property, O object);

	/**
	 * Writes a triple in the store.
	 * @param subject
	 * The subject of the the triple.
	 * @param property
	 * The property of the triple.
	 * @param object
	 * The object of the triple.
	 * @param <S>
	 * Type of the subject of the triple.
	 * @param <P>
	 * Type of the property of the triple.
	 * @param <O>
	 * Type of the object of the triple.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 */
	<S, P, O> void write(S subject, P property, O object)
			throws IOException;

	/**
	 * Reads the properties and objects constituting triples with a specific  
	 * subject.
	 * @param subject
	 * The subject to list the properties and objects for.
	 * @return Properties of the specified subject mapped with the 
	 * associated objects.
	 * @param <S>
	 * Type of the subject of the triple.
	 * @param <P>
	 * Type of the property of the triple.
	 * @param <O>
	 * Type of the object of the triple.
	 */
	<S, P, O> Map<P, List<O>> read(S subject);
	
	/** 
	 * Starts a transaction (if the the triples store supports the feature). 
	 */
	void startTransaction();
	
	/** 
	 * Commits an active transaction (if the the triples store supports the 
	 * feature). 
	 */
	void commitTransaction();
	
	/** 
	 * Commits an active transaction (if the the triples store supports the 
	 * feature). 
	 */
	void rollbackTransaction();
	
	/**
	 * Sets an autocommit interval (if the triples store supports the feature).
	 * @param writeInterval
	 * The autocommit interval.
	 */
	void setAutocommitInterval(Integer writeInterval);
	
	/**
	 * Returns the autocommit interval (if the triples store supports the 
	 * feature).
	 * @return The autocommit interval (default is {@code 1}).
	 */
	Integer getAutocommitInterval();
	
	/**
	 * Closes all resources of the triples store used by the implementation.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 */
	void close() throws IOException;

	/**
	 * Clears all the the triples store content.
	 * <p><b>Beware</b>, this operation might be very long.</p>
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 */
	void clear() throws IOException;
}