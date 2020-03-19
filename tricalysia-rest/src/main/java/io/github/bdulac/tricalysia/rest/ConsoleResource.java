package io.github.bdulac.tricalysia.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import com.sun.jersey.api.view.Viewable;

@Path("/browse")
@Provider
public class ConsoleResource {
	
	@GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response get() {
		return Response.ok(new Viewable("/browse")).build();
    }

}
