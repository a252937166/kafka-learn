package com.ouyang;

import java.util.*;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaConsumerUtil { 
	private Properties props = null;
    public void consume(String topic) {
    	if(null == props)
    		props = getConfig();
    	
    	Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);  
    	//订阅主题列表topic
    	consumer.subscribe(Collections.singletonList(topic));
	    
	    //【1】只获取一次
//	    ConsumerRecords<String, String> records = consumer.poll(1000); 
//	    System.out.println(records.count()); 
//	    int flag = 0;
//	    Set<TopicPartition> set = new HashSet<TopicPartition>();
//	    for (ConsumerRecord<String, String> record : records) {  
//	    	System.out.print("[Info]");
//	        System.out.println(record);
//	        Long timestamp = record.timestamp();
//	        //
////	        if(flag == 0){
////    	        set.add(new TopicPartition(record.topic(), record.partition()));
////    	        consumer.seekToBeginning(set);
////    	        flag = 1;
////	        }
//	    }
//	    consumer.close();
	    
//	    //【2】不断监听新消息
	    while (true) {
	        ConsumerRecords<String, String> records = consumer.poll(100);
	        for (ConsumerRecord<String, String> record : records)
	            //　正常这里应该使用线程池处理，不应该在这里处理
	            System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()+"\n");
	        try {
				Thread.sleep(1 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	
	    }
    }  
    
    private static Properties getConfig() {
    	Properties props = new Properties();  
    	props.put("bootstrap.servers", "localhost:9092");  
    	props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
    	props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  

    	//消费者的组id
    	props.setProperty("group.id", "0");  
    	props.setProperty("enable.auto.commit", "true"); 
//    	props.put("auto.commit.interval.ms", "1000");
//    	//从poll(拉)的回话处理时长
//    	props.put("session.timeout.ms", "30000");
//    	//poll的数量限制
//    	props.put("max.poll.records", "100");
    	props.setProperty("auto.offset.reset", "earliest");
    	return props;
    }
    
    public static void main(String[]args) {
    	KafkaConsumerUtil consumer = new KafkaConsumerUtil();
    	System.out.println("=======consumer1=======");
    	consumer.consume("test");
//    	try {
//			Thread.sleep(20*1000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//    	System.out.println("After 20 seconds.");
//    	consumer.consume("test");
//    	return;
    }
}  