package io.github.bdulac.tricalysia.spark;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.opencsv.CSVWriter;

import io.github.bdulac.tricalysia.AbstractTricalysia;

/** 
 * Implementation of the triples store using a single HDFS file, taking 
 * advantage of <em>Apache Spark</em>.
 */
public class SparkTricalysia extends AbstractTricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(SparkTricalysia.class.getName());
	
	/** <em>Apache Spark</em> context. */
	private JavaSparkContext sparkContext;
	
	/** HDFS store file path. */
	private String filePath;
	
	/** HDFS configuration. */
	private Configuration hadoopConf;
	
	/** HDFS file system loaded with the specified configuration. */
	private FileSystem hdfs;
	
	/** HDFS store file output stream */
	private FSDataOutputStream oStream;
	
	private boolean isAppendable;
	
	/**
	 * Construction of a triples store using HDFS using the default 
	 * HDFS configuration.
	 * @param sc
	 * <em>Apache Spark</em> context.
	 * @param fPath
	 * HDFS store file path.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	public SparkTricalysia(JavaSparkContext sc, String fPath) 
			throws IOException, URISyntaxException {
		this(sc, fPath, null);
	}
	
	/**
	 * Construction of a triples store using HDFS using a specific 
	 * HDFS configuration.
	 * @param sc
	 * <em>Apache Spark</em> context.
	 * @param fPath
	 * HDFS store file path.
	 * @param hConf
	 * HDFS configuration.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	public SparkTricalysia(
			JavaSparkContext sc, 
			String fPath, 
			Configuration hConf
	) throws IOException, URISyntaxException {
		super();
		if(sc == null) {
			throw new IllegalArgumentException();
		}
		if(fPath == null) {
			throw new IllegalArgumentException();
		}
		setAutocommitInterval(1);
		sparkContext = sc;
		filePath = fPath;
		if(hConf == null) {
			hConf = new Configuration();
		}
		hadoopConf = hConf;
		hadoopConf.setBoolean("dfs.support.append", true);
		hadoopConf.set(
				"dfs.client.block.write.replace-datanode-on-failure.policy", 
				"ALWAYS"
		);
		hadoopConf.setBoolean(
				"dfs.client.block.write.replace-datanode-on-failure.best-effort", 
				true
		);
		hadoopConf.setBoolean(
				"spark.hadoop.outputCommitCoordination.enabled", 
				false
		);
	}
	
	public void setFilePath(String fPath) {
		filePath = fPath;
	}
	

	/** 
	 * Preparing the HDFS file system with the specified configuration and 
	 * opening the output stream on the store file.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	protected void initHdfsSession() throws IOException, URISyntaxException {
		if(hdfs == null) {
			hdfs = FileSystem.get(new URI(filePath), hadoopConf);
			isAppendable = 
					Boolean.valueOf(hdfs.getConf().get("dfs.support.append"));
		}		
		Path p = new Path(filePath);
		if(!hdfs.exists(p)) {
			hdfs.createNewFile(p);
		}
		if(isAppendable && (oStream == null)) {
			oStream = hdfs.append(p);
		}
		else if(!isAppendable) {
			oStream = hdfs.create(p);
		}
	}
	
	@Override
	public <N> boolean exists(N node) {
		String n = toObjectString(node);
		JavaRDD<String> fileRdd = sparkContext.textFile(filePath)
				.filter(new TripleFilter(n));
		return !fileRdd.isEmpty();
	}
	
	@Override
	public <S, P, O> boolean exists(S subject, P property, O object) {
		String s = toSubjectString(subject);
		String p = toSubjectString(property);
		String o = toSubjectString(object);
		JavaRDD<String> fileRdd = sparkContext.textFile(filePath)
				.filter(new TripleFilter(s, p, o));
		return !fileRdd.isEmpty();
	}

	@Override
	public <S, P, O> void write(S subject, P property, O object) 
			throws IOException {
		String s = toObjectString(subject);
		String p = toObjectString(property);
		String o = toObjectString(object);
		try {
			startAbstractTransaction();
			Path path = new Path(filePath);
			try {
				if(hdfs.exists(path)) {
					StringBuffer sb = new StringBuffer();
					sb.append(s);
					sb.append("\t");
					sb.append(p);
					sb.append("\t");
					sb.append(o);
					if(isAppendable) {
						String[] tr = new String[3];
						tr[0] = s;
						tr[1] = p;
						tr[2] = o;
						PrintWriter writer = new PrintWriter(oStream);
						@SuppressWarnings("resource")
						CSVWriter csvWriter = new CSVWriter(writer, ',', '"', '"', "\n");
						csvWriter.writeNext(tr,true);
				        // writer.append("\n\r"+ sb.toString());
						csvWriter.flush();
				        writer.flush();
					}
					else {
						byte[] b = ("\n\r"+ sb.toString()).getBytes();
						oStream.write(b);
					}
					commitAbstractTransaction();
				}
				else {
					logger.severe("File " + filePath + " does not exist");
					hdfs.close();
					System.exit(-1);
				}
			} catch(IOException e) {
				initHdfsSession();
				throw e;
			}
		} catch(URISyntaxException e) {
			throw new IllegalStateException(
					"Malformed URI (" + filePath 
					+ "), triple store not available"
			);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <S> List<S> subjects() {
		JavaRDD<String> rddLines = sparkContext.textFile(filePath);
		List<String> triplesLines = rddLines.collect();
		List<S> subjects = new ArrayList<S>();
		for(String tripleLine : triplesLines) {
			String[] triple = TripleFilter.parseRfc4180Line(tripleLine);
			if(triple.length != 3) {
				logger.warning("Illegal triple: " + triple);
			}
			else {
				String subject = triple[0];
				subjects.add((S)subject);
				/*
				String b = triple[1];
				String c = triple[2];
				*/
			}
		}
		return subjects;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S, P, O> Map<P, List<O>> read(S subject) {
		String s = toObjectString(subject);
		Map<P, List<O>> properties = new HashMap<P, List<O>>();
		JavaRDD<String> rddLines = sparkContext.textFile(filePath);
		Function<String, Boolean> filter = new TripleFilter(s);
		JavaRDD<String> subjectLines = 
				rddLines.filter(filter);
		List<String> triplesLines = subjectLines.collect();
		for(String tripleLine : triplesLines) {
			// try {
				// String[] triple = parser.parseLine(tripleLine);
				String[] triple = TripleFilter.parseRfc4180Line(tripleLine);
				if(triple.length != 3) {
					logger.warning("Illegal triple: " + String.join(",", triple));
				}
				else {
					String a = triple[0];
					String b = triple[1];
					String c = triple[2];
					List<O> objects = null;
					O o = null;
					P key = null;
					if(s.equals(a)) {
						key = (P)b;
						objects = properties.get(key);
						if(objects == null) {
							objects = new ArrayList<O>();
						}
						o = (O)c;
					}
					else if(s.equals(b)) {
						key = (P)a;
						objects = properties.get(key);
						if(objects == null) {
							objects = new ArrayList<O>();
						}
						o = (O)c;
					}
					else if(s.equals(c)) {
						key = (P)a;
						objects = properties.get(key);
						if(objects == null) {
							objects = new ArrayList<O>();
						}
						o = (O)b;
					}
					if(!objects.contains(o)) {
						objects.add(o);
					}
					properties.put(key, objects);
				}
			/*	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
		return properties;
	}

	@Override
	public void clear() throws IOException {
		try {
			try {
				Path p = new Path(filePath);
				if(hdfs == null) {
					initHdfsSession();
				}
				if(hdfs.exists(p)) {
					oStream.close();
					oStream = null;
					hdfs.delete(p, true);
					hdfs.createNewFile(p);
					if(isAppendable && (oStream == null)) {
						oStream = hdfs.append(p);
					}
					else if(!isAppendable) {
						oStream = hdfs.create(p);
					}
				}
			} catch(IOException e) {
				initHdfsSession();
				throw e;
			}
		} catch (URISyntaxException e) {
			throw new IllegalStateException(
					"Malformed URI (" + filePath 
					+ "), triple store not available"
			);
		}
		
	}

	@Override
	public void startTransaction() {
		try {
			initHdfsSession();
		} catch (IOException | URISyntaxException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void commitTransaction() {
		try {
			if(!isAppendable) {
				if(oStream != null) {
					oStream.close();
				}
				if(hdfs != null) {
					hdfs.close();
				}
			}
			else {
				logger.fine("TRANSACTION ->" + currentInterval);
				oStream.hflush();
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}
	
	@Override
	public void close() throws IOException {
		commitTransaction();
		// Path p = new Path(filePath);
		if(oStream != null) {
			oStream.close();
		}
		if(hdfs != null) {
			hdfs.close();
		}
	}

	@Override
	public void rollbackTransaction() {
		throw new UnsupportedOperationException();
	}
	
	
}