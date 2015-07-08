// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   KafkaSink.java

package com.zyy.bi.flume.sink;

import com.zyy.bi.flume.util.KafkaUtil;
import com.google.common.base.Preconditions;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSink extends AbstractSink implements Configurable {

	public static final String CUSTOME_TOPIC_KEY_NAME = "topic";
	public static final String PARTITION_KEY_NAME = "custom.partition.key";
	public static final String ENCODING_KEY_NAME = "custom.encoding";
	public static final String DEFAULT_ENCODING = "UTF-8";
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	private String topic;
	private String partitionKey;
	private Producer producer;
	private String encoding;

	public KafkaSink() {

	}

	/**
	 * Pull events out of channel and send it to Kafka
	 */
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		try {
			tx.begin();
			Event event = channel.take();
			String eventData = new String(event.getBody(), encoding);
			log.trace("Message: {}", eventData);
			KeyedMessage<String, String> message = null;
			if (partitionKey != null || partitionKey.equals("")) {
				message = new KeyedMessage<String, String>(topic, event.getHeaders().get("partitionKey"),eventData);
			} else {
				message = new KeyedMessage<String, String>(topic, eventData);
			}
			producer.send(message);
			tx.commit();
			status = Status.READY;
		} catch (Exception e) {
			// TODO: handle exception
			log.error("KafkaSink Exception:{}", e);
			tx.rollback();
			status = Status.BACKOFF;
		} finally {
			tx.close();
		}
		return status;
	}

	/**
	 * read configuration
	 * custom.partition.key Event Heard
	 */
	@Override
	public void configure(Context context) {
		topic = Preconditions.checkNotNull(
				context.getString(CUSTOME_TOPIC_KEY_NAME), "topic is required");
		Preconditions.checkNotNull(context.getString("metadata.broker.list"),
				"metadata.broker.list is required");
		partitionKey = context.getString(PARTITION_KEY_NAME);
		encoding = StringUtils.defaultIfEmpty(
				context.getString(ENCODING_KEY_NAME), DEFAULT_ENCODING);
		producer = KafkaUtil.getProducer(context);
	}

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		super.stop();
	}

}
