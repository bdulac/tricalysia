package io.github.bdulac.tricalysia.rest.config;

import javax.ws.rs.ApplicationPath;

import com.sun.jersey.api.core.PackagesResourceConfig;

@ApplicationPath("/")
public class AppConfig extends PackagesResourceConfig {
	
	public AppConfig() {
		super("io.github.bdulac.tricalysia.rest");
	}
}
