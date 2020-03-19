package io.github.bdulac.tricalysia;

public interface TripleStoreService <T extends Tricalysia> {
	
	public Class<T> getSupportedClass();
	
	public T setupTripleStore(Class<T> cl);
}
