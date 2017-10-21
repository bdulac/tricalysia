package io.github.bdulac.tricalysia;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.logging.Logger;

public abstract class AbstractTricalysia implements Tricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractTricalysia.class.getName());
	
	protected TriplesExtractor urlExtractor;
	
	public AbstractTricalysia() {
		try {
			ServiceLoader<TriplesExtractor> loader = 
					ServiceLoader.load(TriplesExtractor.class);
			Iterator<TriplesExtractor> it = loader.iterator();
			if(it.hasNext()) {
				urlExtractor = it.next();
			}
			else {
				String msg = 
						"No triple extractor available. "
								+ "Please specify a Service Provider Interface (SPI) implementation.";
				logger.severe(msg);
				throw new IllegalStateException(msg);
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
	
	public AbstractTricalysia(TriplesExtractor extractor) {
		if(extractor == null) {
			throw new IllegalArgumentException();
		}
		urlExtractor = extractor;
	}
	
	@Override
	public TriplesExtractor getUrlExtractor() {
		return urlExtractor;
	}
}