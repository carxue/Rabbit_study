package com.rabbit.producer.transaction9;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;

public class TransactionSend {
	public static void main(String[] args) {
		String exchangeName = "confirmExchange";
		String queueName = "confirmQueue";
		String routingKey = "confirmRoutingKey";
		String bindingKey = "confirmBindingKey";
		int count = 1;

		ConnectionFactory factory = new ConnectionFactory();

		// 创建生产者
		Sender1 producer = new Sender1(factory, count, exchangeName, queueName, routingKey, bindingKey);
		producer.run();
	}
}

class Sender1 {
	private ConnectionFactory factory;
	private int count;
	private String exchangeName;
	private String queueName;
	private String routingKey;
	private String bindingKey;

	public Sender1(ConnectionFactory factory, int count, String exchangeName, String queueName, String routingKey,
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

			/**
			 * 如果在没有创建队列前将queueBind注释掉，就会因为找不到队列而返回发送过去的数据并丢弃
			 */

			// channel.confirmSelect();
			// 创建exchange
			channel.exchangeDeclare(exchangeName, "direct", true, false, null);
			// 创建队列
			channel.queueDeclare(queueName, true, false, false, null);
			// 绑定exchange和queue
			channel.queueBind(queueName, exchangeName, routingKey);
			// 发送持久化消息
			for (int i = 0; i < count; i++) {
				// 第一个参数是exchangeName(默认情况下代理服务器端是存在一个""名字的exchange的,
				// 因此如果不创建exchange的话我们可以直接将该参数设置成"",如果创建了exchange的话
				// 我们需要将该参数设置成创建的exchange的名字),第二个参数是路由键
				try {
					//添加事物
					channel.txSelect();
					channel.basicPublish(exchangeName, routingKey, true, MessageProperties.PERSISTENT_BASIC,
							("第" + (i) + "条消息").getBytes());
					channel.addReturnListener(new ReturnListener() {
						
						public void handleReturn(int arg0, String arg1, String arg2, String arg3, BasicProperties arg4,
								byte[] arg5) throws IOException {
							// 此处便是执行Basic.Return之后回调的地方
							String message = new String(arg5);
							System.out.println("Basic.Return返回的结果replyCode:" + arg0 + " replyText:" + arg2 + " routingKey:"
									+ arg3 + " body:" + message);
						}
					});
					channel.txCommit();
				} catch (Exception e) {
					e.printStackTrace();
					channel.txRollback();
				}
			}

			channel.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
