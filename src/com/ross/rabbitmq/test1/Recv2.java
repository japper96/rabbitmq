package com.ross.rabbitmq.test1;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Recv2 {
	private final static String QUEUE_NAME = "Hello!"; //����һ����������
	public static void main(String[] args) {
		ConnectionFactory factory = new ConnectionFactory(); //��ʼ�����ӹ���
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");
		factory.setPassword("0000");
		Connection connection = null ;
		Channel channel = null;
		
		try {
			connection = factory.newConnection(); //��������
			channel = connection.createChannel(); //�����ŵ�
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);//�������С�����������򴴽�
//			String message = "Hello world!";//��Ҫ���͵�����
//			channel.basicPublish("", QUEUE_NAME, null, message.getBytes());//���͵�����
			System.out.println("wait message ...");
			Consumer consumer = new DefaultConsumer(channel){
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					super.handleDelivery(consumerTag, envelope, properties, body);
					String message = new String(body,"UTF-8");
					System.out.println("Received '"+ message + "'");
				}
			};
			
			channel.basicConsume(QUEUE_NAME,true, consumer);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
//			try {
//				channel.close();
//				connection.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (TimeoutException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}

	}

}
