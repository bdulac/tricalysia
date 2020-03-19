package io.github.bdulac.tricalysia;

import java.util.Iterator;
import java.util.ServiceLoader;


/**
 * Triples stores setup using the SPI service loader..
 */
public class TricalysiaTripleStore {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T extends Tricalysia> T setupTripleStore(Class<T> cl) {
		T result = null;
		ServiceLoader<TripleStoreService> loader = 
				ServiceLoader.load(TripleStoreService.class);
		Iterator<TripleStoreService> it = loader.iterator();
		while(it.hasNext()) {
			TripleStoreService<?> next = it.next();
			if(next.getSupportedClass().equals(cl)) {
				TripleStoreService<T> concreteStorage = (TripleStoreService<T>)next;
				result = concreteStorage.setupTripleStore(cl);
				return result;
			}
		}
		throw new IllegalArgumentException(cl.getName());
		
	}
}