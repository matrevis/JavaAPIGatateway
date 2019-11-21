package it.trevis.apigateway;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

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

	@Context
	private HttpHeaders httpHeader;

	@Context
	private HttpServletRequest httpRequest;

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void runProducer(final int sendMessageCount) throws Exception {
		final Producer<Long, String> producer = createProducer();
		long time = System.currentTimeMillis();

		try {
			for (long index = time; index < time + sendMessageCount; index++) {
				final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, "Hello Mom " + index);

				RecordMetadata metadata = producer.send(record).get();

				long elapsedTime = System.currentTimeMillis() - time;
				logger.info("sent record(key={} value={}), metadata(partition={}, offset={}), time={}", record.key(),
						record.value(), metadata.partition(), metadata.offset(), elapsedTime);
			}
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
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getAll() {
		logger.debug("Chiamata a getAll()..");
		logger.debug("HTTP Header: {}", httpHeader.getRequestHeaders().toString());
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le
		 * Request Il microservizio processa la Request e va in db postgresql ad
		 * estrarre i dati I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json
		 * al chiamante
		 */

		return Response.ok(jsonb.toJson("risposta"), MediaType.APPLICATION_JSON).build();
//		List<Integer> result = null;
//		return Response.ok(jsonb.toJson(result), MediaType.APPLICATION_JSON).build();
	}

	@POST
	@Path("/")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postAll() {
		logger.info("Chiamata a postAll()..");
		logger.debug("HTTP Header: {}", httpHeader.getRequestHeaders().toString());
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(httpRequest.getInputStream()));
			StringBuilder out = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				out.append(line);
			}
			logger.debug("ROW: {}", out.toString());
			reader.close();
			logger.debug("END");
		} catch (IOException e) {
			logger.error("Errore IO..", e);
		}
		/*
		 * Scrivere nel topic kafka associato al microservizio HelloWorldRS per le
		 * Request Il microservizio processa la Request e va in db postgresql ad
		 * estrarre i dati I dati estratti vengono messi nel topic kafka di Response
		 * Poll sul topic kafka di Res e quando ho i dati li ritorno come response json
		 * al chiamante
		 */

		return Response.ok(jsonb.toJson("risposta"), MediaType.APPLICATION_JSON).build();
	}

}
