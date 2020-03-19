package io.github.bdulac.tricalysia;

import java.util.EnumSet;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.MultiException;

import com.sun.jersey.spi.container.servlet.ServletContainer;


public class Console implements Runnable {
	
	private Server jettyServer;
	
	private ServletContextHandler context;
	
	private Boolean starting;
	
	public Console(int port) {
		context 
			= new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		starting = null;
		jettyServer = new Server(port);
		jettyServer.setHandler(context);
		// ServletHolder jerseyHolder = context.addServlet(ServletContainer.class, "/*");
		FilterHolder jerseyHolder = context.addFilter(ServletContainer.class, "/*", EnumSet.of(DispatcherType.REQUEST));
		// jerseyHolder.setInitOrder(0);
		jerseyHolder.setInitParameter(
				"javax.ws.rs.Application",
				"io.github.bdulac.tricalysia.rest.config.AppConfig"
		);
		jerseyHolder.setInitParameter(
				"com.sun.jersey.config.property.JSPTemplatesBasePath",
		        "/WEB-INF/jsp"
		);
		jerseyHolder.setInitParameter( 
				"com.sun.jersey.config.property.packages", 
				"io.github.bdulac.tricalysia.rest"
		);
		jerseyHolder.setInitParameter("allowedOrigins", "*");
	}
	
	public boolean isStarting() {
		return starting != null ? starting : Boolean.TRUE;
	}
	
	public void start() {
		try {
			starting = Boolean.TRUE;
			jettyServer.start();
		} catch (MultiException m) { 
			for(Throwable e : m.getThrowables()) {
				System.err.println(
						"Error running ServerConfig service: " 
						+ e.getMessage()
				); 
			}
		} catch (Exception e) {
			System.err.println(
					"Error running ServerConfig service: "
					+ e.getMessage()
			); 
		} finally {
			starting = Boolean.FALSE;
		}
	}
	
	public void stop() {
		try { 
			jettyServer.join(); 
		} catch (InterruptedException e) {
			System.err.println(
					"Error running ServerConfig service: "
					+ e.getMessage()
			); 
		} finally {
			if(!jettyServer.isStopped()) {
				jettyServer.destroy();
				jettyServer = null;
				starting = null;
			}
		}
	}
	
	@Override
	public void finalize() {
		if(jettyServer != null) {
			stop();
		}
	}
	
	@Override
	public void run() {
		start();
	}
	
	public static void main(String[] args) {
		if(args.length == 0) {
			System.err.println(
					"Port should be specified as a first argument"
			);
		}
		else {
			String intStr = args[0];
			try {
				int port = Integer.parseInt(intStr);
				Console c = new Console(port);
				Thread th = new Thread(c);
				th.setDaemon(false);
				th.start();
			} catch(NumberFormatException e) {
				System.err.println(
						"Port should be specified as a first argument"
				);
			}
		}
	}
}