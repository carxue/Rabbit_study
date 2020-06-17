package com.rabbit.consumer.rpc6;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RPCClient {

	private Connection connection;
	private Channel channel;
	private String requestQueueName = "rpc_queue";
	private String replyQueueName;

	public RPCClient() throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare().getQueue();
	}

	public String call(String message) throws IOException, InterruptedException {
		final String corrId = UUID.randomUUID().toString();
		System.out.println("corrId:"+corrId);
		AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName)
				.build();

		channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

		final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);

		channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
					byte[] body) throws IOException {//envelope信封、包层
				if (properties.getCorrelationId().equals(corrId)) {
					response.offer(new String(body, "UTF-8"));//a队列插入数据:队列未满时，返回true；队列满时返回false。非阻塞立即返回
				}
			}
		});

		return response.take();//删除队列中的值:队列不为空返回队首值并移除；当队列为空时会阻塞等待，一直等到队列不为空时再返回队首值
	}

	public void close() throws IOException {
		connection.close();
	}

	public static void main(String[] argv) {
		RPCClient fibonacciRpc = null;
		String response = null;
		try {
			fibonacciRpc = new RPCClient();

			System.out.println(" [x] Requesting fib(30)");
			response = fibonacciRpc.call("30");
			System.out.println(" [.] Got '" + response + "'");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (fibonacciRpc != null) {
				try {
					fibonacciRpc.close();
				} catch (IOException _ignore) {
				}
			}
		}
	}
}