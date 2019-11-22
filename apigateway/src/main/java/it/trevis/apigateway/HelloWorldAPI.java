package it.trevis.apigateway;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.JsonObject;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/hello")
public class HelloWorldAPI {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final static String TOPIC = "test";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
//            "localhost:9092,localhost:9093,localhost:9094";

	Jsonb jsonb = JsonbBuilder.create();
	
	Base64.Encoder enc = Base64.getEncoder();

	@Context
	private Request req;

	@Context
	protected UriInfo uriInfo;

	@Context
	private HttpHeaders httpHeader;

	@Context
	private HttpServletRequest httpRequest;
	
	private static Producer<Long, Map<String,Object>> createMyProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void runMyProducer(Map<String, Object> mapToTopic) throws Exception {
		final Producer<Long, Map<String, Object>> producer = createMyProducer();
		long time = System.currentTimeMillis();

		try {
			final ProducerRecord<Long, Map<String, Object>> record = new ProducerRecord<>(TOPIC, time, mapToTopic);
			RecordMetadata metadata = producer.send(record).get();
			logger.info("sent record(key={} value={}), metadata(partition={}, offset={}), time={}", record.key(),
					record.value(), metadata.partition(), metadata.offset(), time);
		} finally {
			producer.flush();
			producer.close();
		}
	}
	
	static void runProducer(String message) throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, time, message);
			RecordMetadata metadata = producer.send(record).get();
			logger.info("sent record(key={} value={}), metadata(partition={}, offset={}), time={}", record.key(),
					record.value(), metadata.partition(), metadata.offset(), time);
		} finally {
			producer.flush();
			producer.close();
		}
	}

	/*
	 * Al momento non è conforme allo standard Rest Le chiamate get devono essere
	 * "cachabili" e sempre uguali dato il medesimo input Per test si utilizzerà
	 * comunque una chiamata get Da girare poi in post per standard Rest
	 */
	@GET
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getAll(@PathParam("id") String id) {
		logger.info("Chiamata a getAll()..");
		logger.info("Request: {}", req.getMethod().toString());
		logger.info("URI info: {}", uriInfo.getPath());
		logger.debug("HTTP Header: {}", httpHeader.getRequestHeaders().toString());
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le
		 * Request Il microservizio processa la Request e va in db postgresql ad
		 * estrarre i dati I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json
		 * al chiamante
		 */
		/*
		 * manca il GET da passare in kafka per dire che è una select
		 * se passo DELETE con id diventa una DELETE per kafka
		 */
		try {
			runProducer(id);
		} catch (Exception e) {
			logger.error("Errore runProducer..", e);
		}
		

		return Response.ok(jsonb.toJson("risposta"), MediaType.APPLICATION_JSON).build();
//		List<Integer> result = null;
//		return Response.ok(jsonb.toJson(result), MediaType.APPLICATION_JSON).build();
	}

	@POST
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postAll() {
		String strbody = "";
		logger.info("Chiamata a postAll()..");
		logger.debug("Request: {}", req.getMethod().toString());
		logger.debug("URI info: {}", uriInfo.getPath());
		logger.debug("HTTP Header: {}", httpHeader.getRequestHeaders().toString());
		try {
//			ServletInputStream is = httpRequest.getInputStream();
			
			/*
			 * creare un json con Method = POST
			 * URI = /hello/
			 * Body = inputStream
			 * Convertirlo in String
			 * Girarlo in byteArray
			 * Fare un base64
			 * Avere un String message per il Producer
			 */
			
			JsonObject jo = Json.createObjectBuilder()
					.add("Method", req.getMethod())
					.add("URI", uriInfo.getPath())
					.add("Body", enc.encodeToString(IOUtils.toByteArray(httpRequest.getInputStream())))
					.build();
			String msg = jo.toString();
			logger.info("JsonObj to String: {}", msg);
			
			try {
				runProducer(msg);
			} catch (Exception e) {
				logger.error("Errore runProducer..", e);
			}
		} catch (IOException e) {
			logger.error("Errore IO in postAll..", e);
		}
		
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le
		 * Request Il microservizio processa la Request e va in db postgresql ad
		 * estrarre i dati I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json
		 * al chiamante
		 */

		return Response.ok(jsonb.toJson(strbody), MediaType.APPLICATION_JSON).build();
	}

}
