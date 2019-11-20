package it.trevis.apigateway;

import java.lang.invoke.MethodHandles;
import java.util.List;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/hello")
public class HelloWorldAPI {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());

	Jsonb jsonb = JsonbBuilder.create();
	
	/*
	 * Al momento non è conforme allo standard Rest
	 * Le chiamate get devono essere "cachabili" e sempre uguali dato il medesimo input
	 * Per test si utilizzerà comunque una chiamata get
	 * Da girare poi in post per standard Rest
	 */
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAll() {
		logger.debug("Chiamata a getAll()..");
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le Request
		 * Il microservizio processa la Request e va in db postgresql ad estrarre i dati
		 * I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json al chiamante
		 */
		
		List<Integer> result = null;
		return Response.ok(jsonb.toJson(result), MediaType.APPLICATION_JSON).build();
	}
	
}
