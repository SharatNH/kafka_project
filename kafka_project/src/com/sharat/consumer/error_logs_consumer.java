package com.sharat.consumer;

import java.util.Arrays;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class error_logs_consumer {
	public static void main(String[] args) throws Exception{

		System.out.println("in error logs consumer");
		String topicName = "status_failure";
		String groupName = "failure_status_group";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("zk.connect", "localhost:2181");
		props.put("group.id", groupName);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


		KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topicName));

		while (true){
			ConsumerRecords<Long, String> records = consumer.poll(100);
			for (ConsumerRecord<Long, String> record : records){
				System.out.println("failure details= " + String.valueOf(record.value()));
			}
		}

	}
}
