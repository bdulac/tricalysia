package io.github.bdulac.tricalysia.gremlin;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import io.github.bdulac.tricalysia.TricalysiaTestUtils;
import io.github.bdulac.tricalysia.gremlin.GremlinTricalysia;
import junit.framework.Assert;
import junit.framework.TestCase;

public class GremlinTricalysiaTest extends TestCase {
	
	public void testTriple() throws IOException {
		GremlinTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(GremlinTricalysia.class);
		tri.clear();
		tri.write("(`aged' pronounced as one syllable); \\\"mature well-aged cheeses\\\")@en-US`{vp}", "my property`", "my object'");
		Assert.assertEquals(2, IteratorUtils.count(tri.getGraph().vertices()));
		Assert.assertEquals(1, IteratorUtils.count(tri.getGraph().edges()));
		Map<Object, List<Object>> result = tri.read("(`aged' pronounced as one syllable); \\\"mature well-aged cheeses\\\")@en-US`{vp}");
		Assert.assertEquals(1, result.size());
		Assert.assertEquals("my property`", result.keySet().iterator().next());
		Assert.assertEquals("my object'", result.values().iterator().next());
		Assert.assertTrue(tri.exists("(`aged' pronounced as one syllable); \\\"mature well-aged cheeses\\\")@en-US`{vp}"));
		Assert.assertTrue(tri.exists("my property`"));
		Assert.assertTrue(tri.exists("my object'"));
		tri.close();
	}
}