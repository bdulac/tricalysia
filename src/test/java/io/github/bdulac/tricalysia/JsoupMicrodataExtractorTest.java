package io.github.bdulac.tricalysia;

import java.util.logging.Logger;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;
import junit.framework.TestCase;

public class JsoupMicrodataExtractorTest extends TestCase {
	
	public void testLoadHtml() throws Exception {
		GremlinTricalysia tri = TricalysiaTestUtils.setupTestStore(GremlinTricalysia.class);
		try {
			tri.clear();
			tri.negociate(
					"https://science.mnhn.fr/institution/mnhn/collection/f/item/j03236"
			);
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(tri.getGraph().vertices()) + " vertices");
			Logger.getAnonymousLogger().info("" + IteratorUtils.count(tri.getGraph().edges()) + " edges");
		} finally {
			tri.close();
		}
	}
}
