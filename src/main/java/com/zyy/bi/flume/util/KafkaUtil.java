// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   KafkaUtil.java

package com.zyy.bi.flume.util;

import java.io.IOException;
import java.util.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtil {

	private static final Logger log = LoggerFactory.getLogger(KafkaUtil.class);

	public KafkaUtil() {
	}

	public static Properties getKafkaConfigProperties(Context context) {
		log.info("context={}", context.toString());
/*		{ parameters:{topic=b5t_log, batchsize=100000, 
 *      metadata.broker.list=10.30.100.203:9092,10.30.100.63:9092,10.30.100.64:9092, 
 * 		type=com.b5m.bi.flume.sink.KafkaSink, 
 * 		serializer.class=kafka.serializer.StringEncoder, 
 * 		producer.type=async, 
 * 		channel=memoryChannel} }
 * 		*/
		Properties props = new Properties();
		Map contextMap = context.getParameters();
		for (Iterator iterator = contextMap.keySet().iterator(); iterator
				.hasNext();) {
			String key = (String) iterator.next();
			if (!key.equals("type") && !key.equals("channel")) {
				props.setProperty(key, context.getString(key));
				log.info("key={},value={}", key, context.getString(key));
			}
		}

		return props;
	}

	public static Producer getProducer(Context context) {
		log.info(context.toString());
		Producer producer = new Producer(new ProducerConfig(
				getKafkaConfigProperties(context)));
		return producer;
	}

	public static ConsumerConnector getConsumer(Context context)
			throws IOException, InterruptedException {
		log.info(context.toString());
		ConsumerConfig consumerConfig = new ConsumerConfig(
				getKafkaConfigProperties(context));
		ConsumerConnector consumer = Consumer
				.createJavaConsumerConnector(consumerConfig);
		return consumer;
	}

}
