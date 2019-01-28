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
 * Զ�̵���(RPC)--4 �ͻ��ˣ�ʹ����������ݣ�
 * 
 * ���ۣ�Ӧ�þ�������ʹ��RPC������ʹ���첽�ܵ���
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
		ConnectionFactory factory = new ConnectionFactory(); //��ʼ�����ӹ���
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");		
		factory.setPassword("0000");
		
		connection = factory.newConnection();
		channel = connection.createChannel();
		replyQueueName = channel.queueDeclare().getQueue();//����һ���ص��Ķ�ռ����
	}
	
	public String call(String message) throws UnsupportedEncodingException, IOException, InterruptedException
	{
		final String corrId = UUID.randomUUID().toString();//Ψһ���֣�������DefaultConsumer��ʵ�ֲ����Ӧ����Ӧ
		BasicProperties props = new BasicProperties()
				.builder()
				.correlationId(corrId)//����1������������Ϣ
				.replyTo(replyQueueName)//����2������������Ϣ
				.build();
		channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));
		
		//�����߽����������ڵ������߳���ִ�У������Ҫ����Ӧ����ǰ��ͣ���̡߳�����ʹ��BlockingQueue�����������Ϊ1��ʾֻ�ȴ�һ����Ӧ��
		final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
		//���Ļص����У�����RPC��Ӧ
		channel.basicConsume(replyQueueName, true,new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
					byte[] body) throws IOException {
				//У��CorrelationId�Ƿ�ƥ��
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
