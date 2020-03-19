package io.github.bdulac.tricalysia.spark;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import io.github.bdulac.tricalysia.TripleStoreService;

public class SparkLocalService implements TripleStoreService<SparkTricalysia> {
	
	private static final SparkConf conf;
	
	private static JavaSparkContext context;
	
	static {
		conf = new SparkConf().setMaster("local").setAppName("triple-store");
	}

	@Override
	public Class<SparkTricalysia> getSupportedClass() {
		return SparkTricalysia.class;
	}
	
	@Override
	public SparkTricalysia setupTripleStore(Class<SparkTricalysia> cl) {
		try {
			/*
			File baseDir = new File("/tmp/hadoop_spark/" ).getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);
			Configuration hConf = new Configuration();
			hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hConf);
			MiniDFSCluster hdfsCluster = builder.build();
			String fPath = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/test.file";
			*/
			String fPath = "hdfs://localhost:8020/test.file";
			if(context == null) {
				context = new JavaSparkContext(conf);
			}
			else {
				context.close();
				context = new JavaSparkContext(conf);
			}
			SparkTricalysia result = new SparkTricalysia(context, fPath);
			return result;
		} catch (IOException | URISyntaxException e) {
			throw new IllegalStateException(e);
		}
	}
}