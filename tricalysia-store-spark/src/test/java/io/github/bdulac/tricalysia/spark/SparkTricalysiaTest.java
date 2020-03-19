package io.github.bdulac.tricalysia.spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.github.bdulac.tricalysia.Tricalysia;
import io.github.bdulac.tricalysia.TricalysiaTripleStore;
import io.github.bdulac.tricalysia.spark.SparkTricalysia;

public class SparkTricalysiaTest {
	
	// private String hdfsURI;
	
	/*
	@BeforeEach
	public void setupEnv() throws IOException {
		System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
		Configuration conf = new HdfsConfiguration();
		File testDataCluster1 = new File("/tmp/hdfs/", "cluster1");
		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDataCluster1.getAbsolutePath());
		// LocalFileSystem fs = FileSystem.getLocal(conf);
		
		Random rand = new Random();
		final int port = 30000 + rand.nextInt(30000);
		// set both of these to the same port. It should fail.
		FileSystem.setDefaultUri(conf, "hdfs://localhost:" + port);
		conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:" + port);
		//DFSTestUtil.formatNameNode(conf);
		// NameNode nameNode = new NameNode(conf);
		
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		builder.nameNodePort(port).manageNameDfsDirs(true).manageDataDfsDirs(true);
		MiniDFSCluster hdfsCluster = builder.build();
		hdfsCluster.waitClusterUp();

		hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
	}*/
	
	
	@Test
	public void testClearWriteRead() throws IOException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
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
		Assertions.assertEquals(1, subjectProperties.size());
		Assertions.assertEquals(
				property, 
				subjectProperties.keySet().iterator().next()
		);
		Assertions.assertEquals(
				object, 
				subjectProperties.values().iterator().next().iterator().next()
		);
		Map<Object, List<Object>> propertyProperties = tri.read(property);
		Assertions.assertEquals(1, propertyProperties.size());
		Assertions.assertEquals(
				subject, 
				propertyProperties.keySet().iterator().next().toString()
		);
		Assertions.assertEquals(
				object, 
				propertyProperties.values().iterator().next().iterator().next()
		);
		Assertions.assertTrue(tri.exists(subject, property, object));
		Assertions.assertTrue(tri.exists(subject));
		Assertions.assertTrue(tri.exists(property));
		Assertions.assertTrue(tri.exists(object));
		tri.close();
	}
	
	@Test
	public void testCloseExists() throws IOException, InterruptedException, URISyntaxException {
		SparkTricalysia tri = 
				TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		tri.clear();
		tri.write("my subject", "my property", "my object");
		Assertions.assertTrue(tri.exists("my subject"));
		Assertions.assertTrue(tri.exists("my property"));
		Assertions.assertTrue(tri.exists("my object"));
		tri.close();
		tri = TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		Assertions.assertTrue(tri.exists("my subject"));		
		tri.close();
		tri = TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		tri.clear();
		Assertions.assertFalse(tri.exists("my object"));	
		tri.close();
		tri = TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		Assertions.assertFalse(tri.exists("my subject"));
		Assertions.assertFalse(tri.exists("my property"));
		Assertions.assertFalse(tri.exists("my object"));
		tri.write("my subject", "my property", "my object");
		Assertions.assertTrue(tri.exists("my subject"));
		Assertions.assertTrue(tri.exists("my property"));
		Assertions.assertTrue(tri.exists("my object"));
		tri.close();
		
	}
	
	@Test
	// @Disabled
	public void testWriteConcurrent() throws IOException, InterruptedException, URISyntaxException {
		boolean concurrent = false;
		SparkTricalysia tri = null;
		while(!concurrent) {
			tri = 
					TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
			concurrent = executeConcurrent(tri);
		}
		tri = TricalysiaTripleStore.setupTripleStore(SparkTricalysia.class);
		Assertions.assertTrue(concurrent);
		List<Object> subjects = tri.subjects();
		for(int i = 0 ; i < 100 ; i++) {
			int index = i + 1;
			String subjA = "sA".concat("" + index);
			if(!subjects.contains(subjA)) {
				Assertions.fail("Subject " + subjA + " is missing");
			}
			String subjB = "sB".concat("" + index);
			if(!subjects.contains(subjB)) {
				Assertions.fail("Subject " + subjB + " is missing");
			}
		}
		Assertions.assertEquals(200, subjects.size());
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