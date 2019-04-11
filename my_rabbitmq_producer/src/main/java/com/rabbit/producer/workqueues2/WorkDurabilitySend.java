package com.rabbit.producer.workqueues2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class WorkDurabilitySend {
	private final static String QUEUE_NAME = "durable_queue";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("10.41.13.129");
		factory.setUsername("xuekui");
		factory.setPassword("xuekui");
		factory.setPort(5672);

		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		boolean durable = true;//服务器是否加固队列，防止服务挂掉后消息丢失
		channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
		String[] parameters1 = { "welcome", "to", "SZ ." };
		String[] parameters2 = { "welcome", "to", "SZ . ." };
		
		for (int i = 0; i < 5; i++) {
			String message = null;
			if(i%2==0)
				message = i+" "+getMessage(parameters1);
			else
				message = i+" "+getMessage(parameters2);
			channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
			System.out.println(i+" [x] Sent '" + message + "'");
		}

		channel.close();
		connection.close();
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
