/***
 * Copyright 2014 - Rick Crawford - https://github.com/rickcrawford
 */

package com.twodotsolutions.kafka.core;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import kafka.common.FailedToSendMessageException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MessageProducer.class);

	private Producer<UUID, String> producer;
	private String topic;

	public MessageProducer(String kafkaBrokerList, String topic)
			throws ConnectionException {

		LOGGER.debug("Creating Message Producer");

		Properties props = new Properties();
    props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", UUIDSerializer.class.getName());
		props.put("metadata.broker.list", kafkaBrokerList);

		List<String> brokers = Arrays.asList(kafkaBrokerList.split(","));
		for (String broker : brokers) {
			LOGGER.debug("checking host: {}", broker);
			String[] values = broker.split(":");
			String host = values[0];
			int port = 9092;
			if (values.length > 1) {
				port = Integer.parseInt(values[1]);
			}

			try (Socket socket = new Socket(host, port)) {
				continue;
			} catch (Exception e) {
				throw new ConnectionException(host, port);
			}
		}

		this.topic = topic;

		ProducerConfig pc = new ProducerConfig(props);
		producer = new Producer<UUID, String>(pc);
	}

	public void create(List<String> events)
			throws FailedToSendMessageException {
		LOGGER.debug("creating list:{}", events);

		List<KeyedMessage<UUID, String>> messages = new ArrayList<KeyedMessage<UUID, String>>();
		for (String message : events) {
			// Note, I am using the UUID, Message as an example. In this case
			// you would not want to
			// randomly generate a new UUID, but use some unique key for your
			// object. This was just
			// an example of how to create a custom serializer, which in this
			// was case UUIDSerializer!
			messages.add(new KeyedMessage<UUID, String>(this.topic, UUID
					.randomUUID(), message));
		}
		producer.send(messages);
	}

	public void create(String message) throws FailedToSendMessageException {
		create(Arrays.asList(message));
	}

	public void close() {
		LOGGER.debug("closing producer...");

		producer.close();
	}

}
