package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.logging.Logger;

public abstract class AbstractTricalysia implements Tricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractTricalysia.class.getName());
	
	protected Map<String, TriplesExtractor> urlExtractors;
	
	private Set<URL> negociated;
	
	public AbstractTricalysia() {
		try {
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
								+ "Please specify a Service Provider Interface (SPI) implementation.";
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
					negociate(url);
				} catch(IOException e) {
					logger.warning(e.getMessage());
				} finally {
					
				}
			}
		}
	}
	
	protected void negociate(String url) throws IOException {
		URL u = new URL(url);
		if(isNegociated(u)) {
			return;
		}
		HttpURLConnection connection = null;
		try {
			connection = (HttpURLConnection)u.openConnection();
			connection.setRequestMethod("HEAD");
			connection.connect();
			String contentType = connection.getContentType();
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
				for(URL link : result) {
					negociate(link.toString());
				}
				logger.info(result.toString());
			}
			else {
				logger.warning(
						"Unsupported content type: " + contentType 
						+ " (URL=" + u + ")"
				);
			}
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

	/**
	 * Returns an extractor of remote triples associate with a specific mime type.
	 * @param mimeType
	 * Mime type to get the extractor for.
	 * @return Triples extractor for the specified mime type.
	 */
	public TriplesExtractor getRemoteTriplesExtractor(String mType) {
		return urlExtractors.get(mType);
	}
}