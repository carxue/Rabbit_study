package com.rabbit.producer.headers7;

import java.util.Hashtable;
import java.util.Map;

import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class HeaderSend {
	private final static String EXCHANGE_NAME = "exchange_header";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		//添加exchange交换器:fanout较好类型还有topic、direct、headers
		channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS,false, true, null);
		String[] parameters1 = { "welcome", "to", "SZ ." };
		
		//设置消息头键值对信息  
        Map<String, Object> headers = new Hashtable<String, Object>();  
        headers.put("name", "jack");  
        headers.put("age", 30);  
        Builder builder = new Builder();  
        builder.headers(headers); 
		
		
		for (int i = 0; i < 1; i++) {
			String message = i+" "+getMessage(parameters1);
			//默认exchange交换器的名字""
			channel.basicPublish(EXCHANGE_NAME, "", builder.build(), message.getBytes("UTF-8"));
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
