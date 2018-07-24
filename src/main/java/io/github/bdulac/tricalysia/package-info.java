/**
 * <em>Tricalysia</em> main components.
 * <ul>
 * 	<li>An interface for accessing basic functions of a triple store.</li>
 * 	<li>An interface for extracting triples from a semantic resource.</li>
 * </ul>
 * {@link TriplesExtractor} is a <em>Service Provider Interface</em> (SPI).
 * Its implementations are loaded via the <em>ServiceLoader</em> mechanism 
 * and are available in the abstract <em>Tricalysia</em> implementation.
 * @see Tricalysia
 * @see TriplesExtractor
 * @see AbstractTricalysia
 */
package io.github.bdulac.tricalysia;