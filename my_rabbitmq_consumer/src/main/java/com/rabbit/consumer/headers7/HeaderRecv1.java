package com.rabbit.consumer.headers7;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class HeaderRecv1 {
	private final static String EXCHANGE_NAME = "exchange_header";
	private static final String QUEUE_NAME = "header_test_queue"; 
	
	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		
		//声明路由名字和类型  
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS, false, true, null);  
        //创建队列  
        channel.queueDeclare(QUEUE_NAME, false, false, true, null); 
		
		//设置消息头键值对信息  
        Map<String, Object> headers = new Hashtable<String,Object>();  
        //这里x-match有两种类型  
        //all:表示所有的键值对都匹配才能接受到消息  
        //any:表示只要有键值对匹配就能接受到消息  
        headers.put("x-match", "all");  
        headers.put("name", "jack");  
        headers.put("age" , 31); 
        headers.put("sex" , 1); 
		
		
	    //设置绑定的路由key实现消息精确路由分发
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "", headers);
		
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		final Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
				try{
					doWork(message);
				}catch(Exception e){
					System.out.println(e.getMessage());
				}finally{
					channel.basicAck(envelope.getDeliveryTag(), false);
					System.out.println(" [x] Done");
				}
			}
		};
		channel.basicConsume(QUEUE_NAME, false, consumer);
	}
	
	private static void doWork(String task) throws InterruptedException {
	    for (char ch: task.toCharArray()) {
	        if (ch == '.') Thread.sleep(1000);
	    }
	}
}
