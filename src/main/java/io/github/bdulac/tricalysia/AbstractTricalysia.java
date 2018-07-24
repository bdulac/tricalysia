package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.logging.Logger;

/** 
 * Abstract implementation of a triples store. 
 * <p>
 * Loads the triples extractors via the SPI ResourceLoader and associates 
 * each extractor with its supported MIME types.
 * </p>
 * <p>
 * Implements the negociation of semantic resources using the triples 
 * extractors and writes each triple in the store.
 * </p>
 * <p>
 * Provides a basic autocommit software support.
 * </p>
 */
public abstract class AbstractTricalysia implements Tricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractTricalysia.class.getName());
	
	protected Map<String, TriplesExtractor> urlExtractors;
	
	private Set<URL> negociated;
	
	protected Integer autocommitInterval;
	
	protected Integer currentInterval;
	
	public AbstractTricalysia() {
		try {
			autocommitInterval = 1;
			currentInterval = 0;
			urlExtractors = new HashMap<String, TriplesExtractor>();
			ServiceLoader<TriplesExtractor> loader = 
					ServiceLoader.load(TriplesExtractor.class);
			Iterator<TriplesExtractor> it = loader.iterator();
			while(it.hasNext()) {
				TriplesExtractor e = it.next();
				register(e);
			}
			if(urlExtractors.size() == 0) {
				String msg = 
						"No triple extractor available. "
						+ "Please specify a Service Provider Interface (SPI) "
						+ "implementation.";
				logger.severe(msg);
				throw new IllegalStateException(msg);
			}
			negociated = new HashSet<URL>();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	private void register(TriplesExtractor e) {
		List<String> mTypes = e.getSupportedMimeTypes();
		for(String mType : mTypes) {
			urlExtractors.put(mType, e);
		}
	}

	public AbstractTricalysia(TriplesExtractor extractor) {
		if(extractor == null) {
			throw new IllegalArgumentException();
		}
		urlExtractors = new HashMap<String, TriplesExtractor>();
		register(extractor);
	}
	
	@Override
	public void negociate(String... urls) {
		for(String url : urls) {
			if(url != null) {
				try {
					List<URL> allLinks = negociate(url);
					if(allLinks != null) {
						for(int i = 0 ; i< allLinks.size() ; i++) {
							URL link = allLinks.get(i);
							List<URL> urlLinks = negociate(link.toString());
							if(urlLinks != null) {
								allLinks.addAll(urlLinks);
							}
						}
					}
				} catch(IOException e) {
					logger.warning(e.getMessage());
				} finally {
					
				}
			}
		}
	}
	
	protected List<URL> negociate(String url) throws IOException {
		URL u = new URL(url);
		if(isNegociated(u)) {
			return null;
		}
		URLConnection connection = null;
		try {
			connection = u.openConnection();
			if(url.startsWith("http")) {
				HttpURLConnection urlConnection = 
						(HttpURLConnection)connection;
				urlConnection.setRequestMethod("HEAD");
				urlConnection.connect();
			}
			else {
				connection.connect();
			}
			String contentType = connection.getContentType();
			if(contentType == null) {
				contentType = "application/rdf+xml";
				logger.warning(
						"Unknown content type (URL=" + u + "), default is "
						+ contentType
				);
			}
			String mimeType = null;
			if(contentType.contains(";")) {
				mimeType = contentType.substring(0, contentType.indexOf(';'));
			}
			else {
				mimeType = contentType;
			}
			TriplesExtractor extractor = getRemoteTriplesExtractor(mimeType);
			if(extractor != null) {
				List<URL> result = extractor.extract(this, u);
				negociated(u);
				return result;
			}
			else {
				logger.warning(
						"Unsupported content type: " + contentType 
						+ " (URL=" + u + ")"
				);
			}
			return null;
		} finally {
			connection.getInputStream().close();
		}
	}
	
	private void negociated(URL u) {
		negociated.add(u);
	}

	private boolean isNegociated(URL u) {
		return negociated.contains(u);
	}
	
	protected <S> String toSubjectString(S subject) {
		String res = subject.toString();
		return res;
	}
	
	protected <P> String toPropertyString(P property) {
		String prop = property.toString();
		if(prop.startsWith("http://www.w3.org")) {
			return prop.substring(prop.lastIndexOf('/'));
		}
		else if(prop.contains("/dc/terms/")) {
			return prop.substring(prop.indexOf("/dc/"));
		}
		return prop;
	}
	
	protected <O> String toObjectString(O object) {
		String res = object.toString();
		return res;
	}

	/**
	 * Returns an extractor of remote triples associate with a specific mime 
	 * type.
	 * @param mType
	 * Mime type to get the extractor for.
	 * @return Triples extractor for the specified mime type.
	 */
	public TriplesExtractor getRemoteTriplesExtractor(String mType) {
		return urlExtractors.get(mType);
	}
	
	public void startAbstractTransaction() {
		if(currentInterval == 0) {
			startTransaction();
		}
		currentInterval++;
	}
	
	public void commitAbstractTransaction() {
		if(currentInterval >= autocommitInterval) {
			commitTransaction();
			currentInterval = 0;
		}
	}

	@Override
	public Integer getAutocommitInterval() {
		return autocommitInterval;
	}

	@Override
	public void setAutocommitInterval(Integer interval) {
		this.autocommitInterval = interval;
	}
	
	// Blank implementation, to be overridden...
	@Override
	public void close() throws IOException {
		
	}
}