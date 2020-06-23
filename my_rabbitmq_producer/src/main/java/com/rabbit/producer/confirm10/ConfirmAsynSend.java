package com.rabbit.producer.confirm10;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;

public class ConfirmAsynSend {
	public static void main(String[] args) {
		String exchangeName = "confirmExchange";
		String queueName = "confirmQueue";
		String routingKey = "confirmRoutingKey";
		String bindingKey = "confirmBindingKey";
		int count = 10;

		ConnectionFactory factory = new ConnectionFactory();
		// factory.setHost("172.16.151.74");
		// factory.setUsername("test");
		// factory.setPassword("test");
		// factory.setPort(5672);

		// 创建生产者
		Sender2 producer = new Sender2(factory, count, exchangeName, queueName, routingKey, bindingKey);
		producer.run();
	}
}

class Sender2 {
	private ConnectionFactory factory;
	private int count;
	private String exchangeName;
	private String queueName;
	private String routingKey;
	private String bindingKey;

	public Sender2(ConnectionFactory factory, int count, String exchangeName, String queueName, String routingKey,
			String bindingKey) {
		this.factory = factory;
		this.count = count;
		this.exchangeName = exchangeName;
		this.queueName = queueName;
		this.routingKey = routingKey;
		this.bindingKey = bindingKey;
	}

	public void run() {
		try {
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();

			final SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());
			channel.confirmSelect();
			channel.addConfirmListener(new ConfirmListener() {
				public void handleAck(long deliveryTag, boolean multiple) throws IOException {
					System.out.println("success===ack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
					if (multiple) {
						confirmSet.headSet(deliveryTag + 1).clear();
					} else {
						confirmSet.remove(deliveryTag);
					}
				}

				public void handleNack(long deliveryTag, boolean multiple) throws IOException {
					System.out.println("fail========Nack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
					if (multiple) {
						confirmSet.headSet(deliveryTag + 1).clear();
					} else {
						confirmSet.remove(deliveryTag);
					}
				}
			});

			// 创建exchange
			channel.exchangeDeclare(exchangeName, "direct", true, false, null);
			// 创建队列
			channel.queueDeclare(queueName, true, false, false, null);
			// 绑定exchange和queue
			channel.queueBind(queueName, exchangeName, bindingKey);
			// 发送持久化消息
			for (int i = 0; i < count; i++) {
				long nextSeqNo = channel.getNextPublishSeqNo();//从1开自增
				// 第一个参数是exchangeName(默认情况下代理服务器端是存在一个""名字的exchange的,
				// 因此如果不创建exchange的话我们可以直接将该参数设置成"",如果创建了exchange的话
				// 我们需要将该参数设置成创建的exchange的名字),第二个参数是路由键
				channel.basicPublish(exchangeName, routingKey, true, MessageProperties.PERSISTENT_BASIC,
						("第:" + (i) + "条消息nextSeqNo:"+nextSeqNo).getBytes());
				confirmSet.add(nextSeqNo);
			}
			channel.addReturnListener(new ReturnListener() {

				public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, BasicProperties properties,
						byte[] body) throws IOException {
					// 此处便是执行Basic.Return之后回调的地方
					String message = new String(body);
					System.out.println("Basic.Return返回的结果replyCode:" + replyCode + " replyText:" + replyText + " routingKey:"
							+ routingKey + " body:" + message);
				}
			});

			channel.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
