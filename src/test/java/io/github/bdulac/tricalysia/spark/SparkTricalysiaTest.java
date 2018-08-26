package io.github.bdulac.tricalysia.spark;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.github.bdulac.tricalysia.Tricalysia;
import io.github.bdulac.tricalysia.TricalysiaTestUtils;
import io.github.bdulac.tricalysia.spark.SparkTricalysia;
import junit.framework.Assert;

public class SparkTricalysiaTest {
	
	@Test
	public void testClearWriteRead() throws IOException {
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
	
	@Test
	public void testCloseExists() throws IOException, InterruptedException {
		SparkTricalysia tri = 
				TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		tri.write("my subject", "my property", "my object");
		Assert.assertTrue(tri.exists("my subject"));
		Assert.assertTrue(tri.exists("my property"));
		Assert.assertTrue(tri.exists("my object"));
		tri.close();
		tri = TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		Assert.assertTrue(tri.exists("my subject"));		
		tri.close();
		tri = TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		tri.clear();
		Assert.assertFalse(tri.exists("my object"));	
		tri.close();
		tri = TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		Assert.assertFalse(tri.exists("my subject"));
		Assert.assertFalse(tri.exists("my property"));
		Assert.assertFalse(tri.exists("my object"));
		tri.write("my subject", "my property", "my object");
		Assert.assertTrue(tri.exists("my subject"));
		Assert.assertTrue(tri.exists("my property"));
		Assert.assertTrue(tri.exists("my object"));
		tri.close();
		
	}
	
	@Test
	public void testWriteConcurrent() throws IOException, InterruptedException {
		boolean concurrent = false;
		SparkTricalysia tri = null;
		while(!concurrent) {
			tri = 
					TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
			concurrent = executeConcurrent(tri);
		}
		tri = TricalysiaTestUtils.setupTestStore(SparkTricalysia.class);
		Assert.assertTrue(concurrent);
		List<Object> subjects = tri.subjects();
		for(int i = 0 ; i < 100 ; i++) {
			int index = i + 1;
			String subjA = "sA".concat("" + index);
			if(!subjects.contains(subjA)) {
				Assert.fail("Subject " + subjA + " is missing");
			}
			String subjB = "sB".concat("" + index);
			if(!subjects.contains(subjB)) {
				Assert.fail("Subject " + subjB + " is missing");
			}
		}
		Assert.assertEquals(200, subjects.size());
		tri.close();
		Thread.sleep(3000);
	}
	
	private boolean executeConcurrent(Tricalysia tri) throws IOException, InterruptedException {
		tri.clear();
		LoopWriter writerA = new LoopWriter("sA", "pA", "oA", 100, tri);
		LoopWriter writerB = new LoopWriter("sB", "pB", "oB", 100, tri);
		Thread thA = new Thread(writerA);
		Thread thB = new Thread(writerB);
		thA.start();
		thB.start();
		boolean concurrent = false;
		while(writerA.isWriting() || writerB.isWriting()) {
			if(writerA.isWriting() && writerB.isWriting()) {
				concurrent = true;
			}
			Thread.sleep(10);
		}
		tri.close();
		return concurrent;
	}

	class LoopWriter implements Runnable {
		
		private String subjectPattern, propertyPattern, objectPattern;
		
		private int loopsCount;

		private Tricalysia triplesStore;
		
		private boolean writing;
		
		LoopWriter(
				String sPattern, String pPattern, String oPattern, 
				int loops, 
				Tricalysia store
		) {
			subjectPattern = sPattern;
			propertyPattern= pPattern;
			objectPattern = oPattern;
			loopsCount = loops;
			triplesStore = store;
			writing = false;
		}
		
		@Override
		public void run() {
			int i = 0;
			writing = true;
			triplesStore.setAutocommitInterval(loopsCount);
			while(i < loopsCount) {
				try {
					int index = i + 1;
					triplesStore.write(subjectPattern + index, propertyPattern + index, objectPattern + index);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
				i++;
			}
			triplesStore.commitTransaction();
			writing = false;
		}
		
		public boolean isWriting() {
			return writing;
		}
	}
}