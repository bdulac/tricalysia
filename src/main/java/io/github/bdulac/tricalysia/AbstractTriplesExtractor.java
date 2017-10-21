package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.util.logging.Logger;

public abstract class AbstractTriplesExtractor implements TriplesExtractor {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractTriplesExtractor.class.getName());
	
	public void extract(Tricalysia store, String... urls) {
		for(String url : urls) {
			if(url != null) {
				try {
					extract(store, url);
				} catch(IOException e) {
					logger.warning(e.getMessage());
				} finally {
					
				}
			}
		}
	}

}
