package io.github.bdulac.tricalysia.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.github.bdulac.tricalysia.TricalysiaTestUtils;
import io.github.bdulac.tricalysia.spark.SparkTricalysia;
import junit.framework.Assert;

public class SparkTricalysiaTest {
	
	@Test
	public void testTriple() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		String subject = 
				"(`aged' pronounced as one syllable); "
				+ "\\\"mature well-aged cheeses\\\")@en-US`{vp}";
		String property = "my property`";
		String object = "my object'";
		tri.startTransaction();
		tri.write(subject, property, object);
		tri.commitTransaction();
		Map<Object, List<Object>> subjectProperties = tri.read(subject);
		Assert.assertEquals(1, subjectProperties.size());
		Assert.assertEquals(
				property, 
				subjectProperties.keySet().iterator().next()
		);
		Assert.assertEquals(
				object, 
				subjectProperties.values().iterator().next().iterator().next()
		);
		Map<Object, List<Object>> propertyProperties = tri.read(property);
		Assert.assertEquals(1, propertyProperties.size());
		Assert.assertEquals(
				subject, 
				propertyProperties.keySet().iterator().next().toString()
		);
		Assert.assertEquals(
				object, 
				propertyProperties.values().iterator().next().iterator().next()
		);
		Assert.assertTrue(tri.exists(subject, property, object));
		Assert.assertTrue(tri.exists(subject));
		Assert.assertTrue(tri.exists(property));
		Assert.assertTrue(tri.exists(object));
		tri.close();
	}

}



