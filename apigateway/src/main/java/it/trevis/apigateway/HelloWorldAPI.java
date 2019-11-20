package it.trevis.apigateway;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/hello")
public class HelloWorldAPI {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	Jsonb jsonb = JsonbBuilder.create();
	
	@Context
	private Request req;
	
	@Context
	private HttpHeaders httpHeader;
	
	@Context
	private HttpServletRequest httpRequest;
	
	/*
	 * Al momento non è conforme allo standard Rest
	 * Le chiamate get devono essere "cachabili" e sempre uguali dato il medesimo input
	 * Per test si utilizzerà comunque una chiamata get
	 * Da girare poi in post per standard Rest
	 */
	@GET
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getAll() {
		logger.debug("Chiamata a getAll()..");
		logger.info("Request: {} and HTTP Header: {}", req.toString(), httpHeader.getRequestHeaders().toString());
		try {
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(httpRequest.getInputStream()));
	        StringBuilder out = new StringBuilder();
	        String line;
	        while ((line = reader.readLine()) != null) {
	            out.append(line);
	        }
	        logger.info("ROW: {}", out.toString());
	        reader.close();
			
			logger.info("END");
		} catch (IOException e) {
			logger.error("Errore IO..", e);
		}
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le Request
		 * Il microservizio processa la Request e va in db postgresql ad estrarre i dati
		 * I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json al chiamante
		 */
		
		return Response.ok(req, MediaType.APPLICATION_JSON).build();
//		List<Integer> result = null;
//		return Response.ok(jsonb.toJson(result), MediaType.APPLICATION_JSON).build();
	}
	
	@POST
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postAll() {
		logger.debug("Chiamata a postAll()..");
		logger.info("Request: {} and HTTP Header: {}", req.toString(), httpHeader.getRequestHeaders().toString());
		try {
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(httpRequest.getInputStream()));
	        StringBuilder out = new StringBuilder();
	        String line;
	        while ((line = reader.readLine()) != null) {
	            out.append(line);
	        }
	        logger.info("ROW: {}", out.toString());
	        reader.close();
			
			logger.info("END");
		} catch (IOException e) {
			logger.error("Errore IO..", e);
		}
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le Request
		 * Il microservizio processa la Request e va in db postgresql ad estrarre i dati
		 * I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json al chiamante
		 */
		
		return Response.ok(jsonb.toJson("risposta"), MediaType.APPLICATION_JSON).build();
	}
	
}
