package io.github.bdulac.tricalysia;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.jupiter.api.Test;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;

public class JsoupMicrodataExtractorTest {
	
	@Test
	public void testLoadHtml() throws IOException {
		GremlinTricalysia tri = TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		try {
			tri.clear();
			tri.negociate(
					"https://developers.googleblog.com/"
			);
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(tri.getGraph().vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(tri.getGraph().edges()) + " edges");
		} finally {
			tri.close();
		}
	}
}
