package com.rabbit.consumer.workqueues2;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class WorkAckRecv1 {
	private final static String QUEUE_NAME = "durable_queue";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		final Channel channel = connection.createChannel();
		//处理消息的复杂度，想服务器声明一个prefetchcount预处理能力，表示当前consuemr可以同时处理几个message，
		//这样服务器在发送消息前会检查consuemr正在处理的message已发送为收到basicAck的有几个，如果操作设置的值就
		//不在往这个consumer上发送
		channel.basicQos(1); // accept only one unack-ed message at a time (see below)
		
		boolean durable = true;//服务器是否加固队列，防止服务挂掉后消息丢失
		
		channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
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
					System.out.println(" [x] Done");
					//basicConsume(x,false),表示consumer收到消息后不会自动反馈服务器说消息已经消费，而是等consumer处理完之后
					//主动调用channel.basicAck告知服务器已经消费完成。所以如果采用这样方式一定要记得basicAck不然消息一直会重复，消耗系统资源
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			}
		};
		boolean autoAck = false;
		//1.被动消费模式，rabbitmq将消息推送给consumer在消费，basicConsume是开启一个线程一直等待
		channel.basicConsume(QUEUE_NAME, autoAck, consumer);
		//2.主动消费模式，consuemr主动到rabbitmq上获取
		//GetResponse response = channel.basicGet(QUEUE_NAME, boolean autoAck);
		
	}
	
	private static void doWork(String task) throws InterruptedException {
	    for (char ch: task.toCharArray()) {
	        if (ch == '.') Thread.sleep(1000);
	    }
	}
}
