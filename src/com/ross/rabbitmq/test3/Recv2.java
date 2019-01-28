package com.ross.rabbitmq.test3;

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
* �����߳�����ʹ��·�ɼ�����ָ������־��Ϣ�������Ĺ����Լ���Ҫ����Ϣ��
* �ŵ�ָ�������������ͣ��������߱���һ�£�
* ��ȡһ��Ĭ�ϵĶ�������
* �ŵ����а󶨽�����
* ���ģ��ȴ���Ϣ��
*/
public class Recv2 {
	private final static String EXCHANGE_NAME = "logs"; //����һ������������
	public static void main(String[] args) {
		ConnectionFactory factory = new ConnectionFactory(); //��ʼ�����ӹ���
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");
		factory.setPassword("0000");
		Connection connection = null ;
//		Channel channel = null;
		
		try {
			connection = factory.newConnection(); //��������
			
			//�����ŵ�1��ģ��ͬʱ�����ŵ�������Ϣ
			Channel channel = connection.createChannel(); //�����ŵ�			
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");//ָ�����������ƺ����ͣ��������߱���һ��
			String queueName = channel.queueDeclare().getQueue(); //��ȡ�ŵ��Ķ���Ĭ������
			channel.queueBind(queueName, EXCHANGE_NAME, "black"); //ָ��·�ɼ�Ϊblack			
			System.out.println("wait message ...");
			final Consumer consumer = new DefaultConsumer(channel){
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
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
			
			//�����ŵ�2
			Channel channel2 = connection.createChannel(); //�����ŵ�			
			channel2.exchangeDeclare(EXCHANGE_NAME, "fanout");//ָ�����������ƺ����ͣ��������߱���һ��
			String queueName2 = channel2.queueDeclare().getQueue(); //��ȡ�ŵ��Ķ���Ĭ������
			channel2.queueBind(queueName2, EXCHANGE_NAME, "");			
			System.out.println("wait message ...");
			final Consumer consumer2 = new DefaultConsumer(channel2){
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					// TODO Auto-generated method stub
					super.handleDelivery(consumerTag, envelope, properties, body);
					String message = new String(body,"UTF-8");
					System.out.println("Received2 '"+ message + "'");
					try
					{
						doWork(message);
					}finally
					{
						System.out.println("Recv2 [x] Done");
					}
				}
			};			
			channel2.basicConsume(queueName2,true, consumer2);//�ȴ���Ϣ����
			
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
