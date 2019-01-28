package com.ross.rabbitmq.test5;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * 远程调用(RPC)--4 客户端（使用生产者身份）
 * 
 * 结论：应该尽量避免使用RPC，尽量使用异步管道。
 * @author Administrator
 *
 */
public class RPCClient {
	private Connection connection;
	private Channel channel;
	private final String requestQueueName = "rpc_queue";	
	private String replyQueueName;
	
	public RPCClient() throws IOException, TimeoutException
	{
		ConnectionFactory factory = new ConnectionFactory(); //初始化连接工厂
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");		
		factory.setPassword("0000");
		
		connection = factory.newConnection();
		channel = connection.createChannel();
		replyQueueName = channel.queueDeclare().getQueue();//声明一个回调的独占队列
	}
	
	public String call(String message) throws UnsupportedEncodingException, IOException, InterruptedException
	{
		final String corrId = UUID.randomUUID().toString();//唯一数字，用于在DefaultConsumer中实现捕获对应的响应
		BasicProperties props = new BasicProperties()
				.builder()
				.correlationId(corrId)//属性1：发布请求消息
				.replyTo(replyQueueName)//属性2：发布请求消息
				.build();
		channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));
		
		//消费者交付处理是在单独的线程中执行，因此需要在响应到达前暂停主线程。这里使用BlockingQueue来解决，容量为1表示只等待一个响应。
		final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
		//订阅回调队列，接收RPC响应
		channel.basicConsume(replyQueueName, true,new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
					byte[] body) throws IOException {
				//校验CorrelationId是否匹配
				if(properties.getCorrelationId().equals(corrId))
				{
					response.offer(new String(body,"UTF-8"));
				}
			}
			
		});
		return response.take();
	}
	
	public void close() throws IOException
	{
		connection.close();
	}
}
