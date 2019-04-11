package com.rabbit.producer.topic5;

import java.util.Random;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class TopicSend {
	private final static String EXCHANGE_NAME = "topic_logs";
	private final static String[] levels={"error","warning","info","debug"};

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("10.41.13.128");
		factory.setUsername("xuekui");
		factory.setPassword("xuekui");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		//添加exchange交换器:fanout较好类型还有topic、direct、headers
		channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
		String[] parameters1 = { "welcome", "to", "SZ ." };
		
		for (int i = 0; i < 10; i++) {
			String level = getLogLevel();
			String message = i+" "+level+" "+getMessage(parameters1);
			//默认exchange交换器的名字""
			channel.basicPublish(EXCHANGE_NAME, "kern.aa.bb", null, message.getBytes("UTF-8"));
			System.out.println(i+" [x] Sent '"+level+" :  " + message + "'");
		}

		channel.close();
		connection.close();
	}
	
	private static String getLogLevel() {
		Random random = new Random();
		return levels[random.nextInt(4)];
	}
	

	private static String getMessage(String[] strings) {
		if (strings.length < 1)
			return " Hello World!";
		return joinStrings(strings, " ");
	}

	private static String joinStrings(String[] strings, String delimiter) {
		int length = strings.length;
		if (length == 0)
			return "";
		StringBuilder words = new StringBuilder(strings[0]);
		for (int i = 1; i < length; i++) {
			words.append(delimiter).append(strings[i]);
		}
		return words.toString();
	}
}
