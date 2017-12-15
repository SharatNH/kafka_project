package com.sharat.producer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.*;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.receiver.Receiver;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;   

public class producer {
	private static Socket socket;


	private static Producer<Long,String> createProducer(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092,localhost:9093");
		props.put("key.serializer","org.apache.kafka.common.serialization.LongSerializer");         
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return  new KafkaProducer <Long,String>(props);
	}


	public static Pattern apacheLogPattern(){
		String ddd = "\\d{1,3}"; 
		String ip = "("+ddd+"\\."+ddd+"\\."+ddd+"\\."+ddd+")?";
		String client = "(\\S+)";                     
		String user = "(\\S+)";
		String dateTime = "(\\[.+?\\])";              
		String request = "\"(.*?)\"";                 
		String status = "(\\d{3})";
		String bytes = "(\\S+)";                   
		String referer = "\"(.*?)\"";
		String agent = "\"(.*?)\"";
		String regex = ip+" "+client+" "+user+" "+dateTime+" "+request+" "+status+" "+bytes+" "+referer+" "+agent;
		//expected format
		//(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})? (\S+) (\S+) (\[.+?\]) "(.*?)" (\d{3}) (\S+) "(.*?)" "(.*?)"
		System.out.println(Pattern.compile(regex));

		return Pattern.compile(regex);    
	}

	public static void setLogLevels(){
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);   
	}


	public static void main(String[] args) throws Exception{

		System.out.println("starting...");
		Producer<Long,String> producer=createProducer();

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));
		setLogLevels();
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream("127.0.0.1", 9999,StorageLevels.MEMORY_AND_DISK_SER);

		Pattern pattern = apacheLogPattern();
		
		
		JavaDStream<String[]> requests=lines.map(x->  {
			Matcher matcher = pattern.matcher(x); 
			System.out.println(x+" "+matcher+" "+matcher.matches());
			if (matcher.matches()) 
			{
				String request = matcher.group(5);
				String[] requestFields = request.toString().split(" ");
				String url;
				if(requestFields[1]!=null)
					url= requestFields[1]; 
				else  
					url="error";
				// url,status,user agent
				String[] result= {url, matcher.group(6), matcher.group(9)};
				return result;
			}
			else { 
				String[] error= {"error","0","error"};
				return error;
			}
		});

		try {
		requests.foreachRDD((rdd,time)->{
			long success_index=0;
			long failure_index=0;
			List<String[]> elements = rdd.collect();
			StringBuffer sb=new StringBuffer();
			for(String[] element : elements) {
				String url=element[0];
				String sCode=element[1];
				String clientAgent=element[2];

				//if error skip!!
				if(url!="error") {
					//kafka connect 
					//topic name===statusCode 
					int statusCode=Integer.parseInt(sCode);
					if(statusCode >= 200 && statusCode < 300) {
						System.out.println("count success: "+success_index+" "+url+" "+statusCode+" "+clientAgent);
						//pass to statusSuccess topic

						//building string that needs to be sent
						//string build will have url, statusCode and clientAgent
						sb.append(url);
						sb.append(statusCode);
						sb.append(clientAgent);

						//producer record to be sent with index in a topic
						ProducerRecord<Long, String> record = new ProducerRecord<>("status_success",success_index,sb.toString());
						producer.send(record);

						//re-initialization
						sb.setLength(0);
						success_index++;

						//metadata of where the data is inserted at a partition for a topic
//						RecordMetadata metadata = producer.send(record).get();
//
//						System.out.printf("sent a record of (key=%s value=%s) meta(partition=%d, offset=%d) \n",
//								record.key(), record.value(), metadata.partition(),
//								metadata.offset());

					}
					else if (statusCode >= 500 && statusCode < 600) {
						System.out.println("count failure: "+failure_index+" "+url+" "+statusCode+" "+clientAgent);
						//pass to statusError topic

						//building string that needs to be sent
						//string build will have url, statusCode and clientAgent
						sb.append(url);
						sb.append(statusCode);
						sb.append(clientAgent);

						//producer record to be sent with index in a topic
						ProducerRecord<Long, String> record = new ProducerRecord<>("status_failure",failure_index,sb.toString());
						producer.send(record);

						//re-initialization
						sb.setLength(0);
						failure_index++;

						//metadata of where the data is inserted at a partition for a topic
						RecordMetadata metadata = producer.send(record).get();

						System.out.printf("sent a record of (key=%s value=%s) meta(partition=%d, offset=%d) \n",
								record.key(), record.value(), metadata.partition(),
								metadata.offset());



					}
				}
				//if error do nothing
			}
		});
		}
		finally {
            producer.flush();
            producer.close();
		}

		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}
}
