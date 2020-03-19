package io.github.bdulac.tricalysia.hdfs;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.opencsv.CSVWriter;

import io.github.bdulac.tricalysia.AbstractTricalysia;

/** 
 * Abstract implementation of the triples store using a single HDFS file.
 */
public abstract class AbstractHdfsTricalysia extends AbstractTricalysia {
	
	private static final Logger logger = 
			Logger.getLogger(AbstractHdfsTricalysia.class.getName());
	
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
	 * @param fPath
	 * HDFS store file path.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	public AbstractHdfsTricalysia( String fPath) 
			throws IOException, URISyntaxException {
		this(fPath, null);
	}
	
	/**
	 * Construction of a triples store using HDFS using a specific 
	 * HDFS configuration.
	 * @param fPath
	 * HDFS store file path.
	 * @param hConf
	 * HDFS configuration.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	public AbstractHdfsTricalysia(
			String fPath, 
			Configuration hConf
	) throws IOException, URISyntaxException {
		super();
		if(fPath == null) {
			throw new IllegalArgumentException();
		}
		setAutocommitInterval(1);
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
		initHdfsSession(fPath);
	}
	
	public void setFilePath(String fPath) throws IOException, URISyntaxException {
		if(hdfs != null) {
			close();
		}
		initHdfsSession(fPath);
	}

	/** 
	 * Preparing the HDFS file system with the specified configuration and 
	 * opening the output stream on the store file.
	 * @throws IOException
	 * If an I/O malfunction occurs.
	 * @throws URISyntaxException
	 * If the URI is malformed.
	 */
	protected void initHdfsSession(String fPath) throws IOException, URISyntaxException {
		filePath = fPath;
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
	
	protected String getFilePath() {
		return filePath;
	}

	@Override
	public synchronized <S, P, O> void write(S subject, P property, O object) 
			throws IOException {
		String s = toObjectString(subject);
		String p = toObjectString(property);
		String o = toObjectString(object);
			startAbstractTransaction();
			Path path = new Path(filePath);
			try {
				if(hdfs == null) {
					throw new IllegalStateException(
							"Malformed URI (" + filePath 
							+ "), triple store not available"
					);
				}
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
				// initHdfsSession();
				throw e;
			}
	}

	@Override
	public void clear() throws IOException {
		if(hdfs == null) {
			throw new IllegalStateException(
					"Malformed URI (" + filePath 
					+ "), triple store not available"
			);
		}
				Path p = new Path(filePath);
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
		
	}

	@Override
	public synchronized void startTransaction() {
		// TODO how ???
	}

	@Override
	public synchronized void commitTransaction() {
		try {
			if(!isAppendable) {
				if(oStream != null) {
					oStream.hflush();
					oStream.close();
				}
				if(hdfs != null) {
					hdfs.close();
				}
			}
			else {
				logger.info("TRANSACTION ->" + currentInterval);
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
			oStream = null;
		}
		if(hdfs != null) {
			hdfs.close();
			hdfs = null;
		}
	}

	@Override
	public void rollbackTransaction() {
		throw new UnsupportedOperationException();
	}
}