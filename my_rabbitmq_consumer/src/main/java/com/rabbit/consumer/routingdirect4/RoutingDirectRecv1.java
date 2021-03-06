package com.rabbit.consumer.routingdirect4;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RoutingDirectRecv1 {
	private final static String EXCHANGE_NAME = "direct_logs";
	private final static String[] levels={"error","warning","info"};
	
	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		
		channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
	    String queueName = channel.queueDeclare().getQueue();
	    //设置绑定的路由key实现消息精确路由分发
	    channel.queueBind(queueName, EXCHANGE_NAME, levels[0]);
		
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
		channel.basicConsume(queueName, false, consumer);
	}
	
	private static void doWork(String task) throws InterruptedException {
	    for (char ch: task.toCharArray()) {
	        if (ch == '.') Thread.sleep(1000);
	    }
	}
}
