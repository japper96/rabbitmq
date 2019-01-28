package com.ross.rabbitmq.test2;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


/**
 * �����߳�����
 * �ŵ�ָ�������������ͣ��������߱���һ�£�
 * ��ȡһ��Ĭ�ϵĶ�������
 * �ŵ����а󶨽�����
 * ���ģ��ȴ���Ϣ��
 * @author Administrator
 *
 */
public class Recv {
	private final static String EXCHANGE_NAME = "logs"; //����һ������������
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
			
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");//ָ�����������ƺ����ͣ��������߱���һ��
			String queueName = channel.queueDeclare().getQueue(); //��ȡ�ŵ��Ķ���Ĭ������
			channel.queueBind(queueName, EXCHANGE_NAME, "");
			
			System.out.println("wait message ...");
			final Consumer consumer = new DefaultConsumer(channel){
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					super.handleDelivery(consumerTag, envelope, properties, body);
					String message = new String(body,"UTF-8");
					System.out.println("Received '"+ message + "'");
					try
					{
						doWork(message);
					}finally
					{
						System.out.println("Recv1 [x] Done");
					}
				}
			};
			
			channel.basicConsume(queueName,true, consumer);//�ȴ���Ϣ����
			
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
	
	private static void doWork(String task)
	{
		for(char ch:task.toCharArray())
		{
			if(ch == 'o')
			{
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
