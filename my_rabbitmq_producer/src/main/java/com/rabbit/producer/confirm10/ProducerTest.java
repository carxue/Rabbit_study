package com.rabbit.producer.confirm10;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.ReturnListener;

public class ProducerTest {
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

class Sender1{
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

	//批量确认
	public void run() {
		try {
//			factory.setHost("10.41.13.128");
//			factory.setUsername("xuekui");
//			factory.setPassword("xuekui");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
			/**
			 * 如果在没有创建队列前将queueBind注释掉，就会因为找不到队列而返回发送过去的数据并丢弃
			 */
			
			
			channel.confirmSelect();
			
			channel.addConfirmListener(new ConfirmListener() {
	            @Override
	            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
	                System.out.println("nack: deliveryTag = " + deliveryTag + " multiple: " + multiple);
	            }
	            @Override
	            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
	                System.out.println("ack: deliveryTag = " + deliveryTag + " multiple: " + multiple);
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
				// 第一个参数是exchangeName(默认情况下代理服务器端是存在一个""名字的exchange的,
				// 因此如果不创建exchange的话我们可以直接将该参数设置成"",如果创建了exchange的话
				// 我们需要将该参数设置成创建的exchange的名字),第二个参数是路由键
				channel.basicPublish(exchangeName, bindingKey, true, MessageProperties.PERSISTENT_BASIC,("第" + (i) + "条消息").getBytes());
				channel.addReturnListener(new ReturnListener() {
					
					public void handleReturn(int arg0, String arg1, String arg2, String arg3, BasicProperties arg4,
							byte[] arg5) throws IOException {
						// 此处便是执行Basic.Return之后回调的地方
						String message = new String(arg5);
						System.out.println("Basic.Return返回的结果replyCode:"+arg0+" replyText:"+arg2+" routingKey:"+arg3+" body:"+ message);
					}
				});
				if(!channel.waitForConfirms()){//这种是普通的确认模式
					//a发送失败可以设置重发
				    System.out.println("send message failed.");
				}
				//批量Confirm模式  channel.waitForConfirmsOrDie();//直到所有信息都发布，只要有一个未确认就会IOException
			}
			
			
			channel.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
