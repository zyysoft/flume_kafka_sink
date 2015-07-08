// Decompiled by Jad v1.5.8e2. Copyright 2001 Pavel Kouznetsov.
// Jad home page: http://kpdus.tripod.com/jad.html
// Decompiler options: packimports(3) fieldsfirst ansi space 
// Source File Name:   KafkaSink.java

package com.zyy.bi.flume.sink;

import java.util.List;

import com.zyy.bi.flume.util.KafkaUtil;
import com.google.common.base.Preconditions;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSink extends AbstractSink implements Configurable {

	private static final String CUSTOME_TOPIC_KEY_NAME = "topic";
	private static final String PARTITION_KEY_NAME = "custom.partition.key";
	private static final String ENCODING_KEY_NAME = "custom.encoding";
	private static final String BATCHSIZE = "batchsize";
	private static final String DEFAULT_ENCODING = "UTF-8";
	private  long defaultBatchSize=100;
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	private long batchSize;
	private List<KeyedMessage<String, String>> messageList;
	private String topic;
	private String partitionKey;
	private Producer producer;
	private String encoding;

	public KafkaSink() {

	}

	/*
	 * Pull events out of channel and send it to Kafka producer.
	 *Take at most batchSize events per Transaction
	 *  */
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		try {
			transaction.begin();
			long txnEventCount = 0;
			messageList.clear();
			for(; txnEventCount < batchSize; txnEventCount++){
				Event event = channel.take();
				if(event==null){
					break;
				}
				String eventData = new String(event.getBody(), encoding);
				log.trace("Message: {}", eventData);
				KeyedMessage<String, String> message = null;
				if (partitionKey != null || partitionKey.equals("")) {
					message = new KeyedMessage<String, String>(topic, event.getHeaders().get("partitionKey"),eventData);
				} else {
					message = new KeyedMessage<String, String>(topic, eventData);
				}
				messageList.add(message);
				//producer.send(message);
			}
			
			 if (txnEventCount > batchSize) {
				 Log.warn("txnEventCount:{txnEventCount} batchSize:{batchSize}>");
		      }  
			  
			if (messageList.size() > 0) {
				producer.send(messageList);
			}
			transaction.commit();
			status = Status.READY;
		} catch (Exception e) {
			// TODO: handle exception
			log.error("KafkaSink Exception:{}", e);
			transaction.rollback();
			status = Status.BACKOFF;
		} finally {
			transaction.close();
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
		batchSize = context.getLong(BATCHSIZE,defaultBatchSize);
		Preconditions.checkArgument(batchSize > 0,
		        "batchSize must be greater than 0");
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
