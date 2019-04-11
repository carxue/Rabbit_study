package com.rabbit.producer.mandatory8;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;

public class MandatorySend {
	public static void main(String[] args) {
		String exchangeName = "confirmExchange";
		String queueName = "confirmQueue";
		String routingKey = "confirmRoutingKey";
		String bindingKey = "confirmBindingKey";
		int count = 3;

		ConnectionFactory factory = new ConnectionFactory();
		// factory.setHost("localhost");
		// factory.setUsername("guest");
		// factory.setPassword("guest");
		// factory.setPort(5672);

		// 创建生产者
		Sender producer = new Sender(factory, count, exchangeName, routingKey);
		producer.run();
	}
}

class Sender {
	private ConnectionFactory factory;
	private int count;
	private String exchangeName;
	private String routingKey;

	public Sender(ConnectionFactory factory, int count, String exchangeName, String routingKey) {
		this.factory = factory;
		this.count = count;
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
	}

	public void run() {
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			// 创建exchange
			channel.exchangeDeclare(exchangeName, "direct", true, false, null);
			// 发送持久化消息
			for (int i = 0; i < count; i++) {
				// 第一个参数是exchangeName(默认情况下代理服务器端是存在一个""名字的exchange的,
				// 因此如果不创建exchange的话我们可以直接将该参数设置成"",如果创建了exchange的话
				// 我们需要将该参数设置成创建的exchange的名字),第二个参数是路由键
				channel.basicPublish(exchangeName, routingKey, true, MessageProperties.PERSISTENT_BASIC,
						("第" + (i + 1) + "条消息").getBytes());
			}
			
			channel.addReturnListener(new ReturnListener() {
				public void handleReturn(int arg0, String arg1, String arg2, String arg3, BasicProperties arg4,
						byte[] arg5) throws IOException {
					// 此处便是执行Basic.Return之后回调的地方
					String message = new String(arg5);
					System.out.println("Basic.Return返回的结果:  " + message);
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
