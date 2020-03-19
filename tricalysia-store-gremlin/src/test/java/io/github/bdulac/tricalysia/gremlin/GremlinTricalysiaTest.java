package io.github.bdulac.tricalysia.gremlin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.bdulac.tricalysia.TricalysiaTripleStore;

public class GremlinTricalysiaTest {

	@Test
    public void testTriple() throws IOException {
        GremlinTricalysia tri = 
            TricalysiaTripleStore.setupTripleStore(GremlinTricalysia.class);
		tri.clear();
		tri.write("(`aged' pronounced as one syllable); \\\"mature well-aged cheeses\\\")@en-US`{vp}", "my property`", "my object'");
        Assertions.assertEquals(2, IteratorUtils.count(tri.getGraph().vertices()));
        Assertions.assertEquals(1, IteratorUtils.count(tri.getGraph().edges()));
        Map<Object, List<Object>> result = tri.read("(`aged' pronounced as one syllable); \\\"mature well-aged cheeses\\\")@en-US`{vp}");
		Assertions.assertEquals(1, result.size());
		Assertions.assertEquals("my property`", result.keySet().iterator().next());
        Assertions.assertEquals("my object'", result.values().iterator().next().iterator().next());
        Assertions.assertTrue(tri.exists("(`aged' pronounced as one syllable); \\\"mature well-aged cheeses\\\")@en-US`{vp}"));
        Assertions.assertTrue(tri.exists("my property`"));
        Assertions.assertTrue(tri.exists("my object'"));
        tri.close();
	}
}
