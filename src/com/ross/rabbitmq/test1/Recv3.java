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

public class Recv3 {
	private final static String QUEUE_NAME1 = "Hello!"; //����һ����������
	private final static String QUEUE_NAME2 = "Hello!durable"; //����һ����������,������Ϣ�־û�����ʹ����Ҳ���ᶪʧ
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
			for (int i = 0 ;i<5;i++)
			{
				final Channel channel = connection.createChannel(); //�����ŵ�
				boolean durable = true; //�־û���Ϣ���У������ߺ�������ͬʱ�޸ģ���������Ѿ��������޸Ĳ�����Ч�����������½���һ����������			
				channel.queueDeclare(QUEUE_NAME2, durable, false, false, null);//�������С�����������򴴽�
				int prefetch = 1;
				channel.basicQos(prefetch);//����һ��ֻ����һ����Ϣ��ֱ���������������Ϣ�ٽ��ա�
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
							doWork(message); //ģ����Ϣ������̣�������Ϻ���ܽ�����Ӧ
						}finally
						{
							System.out.println("Recv3 [x] Done");
							channel.basicAck(envelope.getDeliveryTag(), false); //��������Ϣ������Ӧ��״̬
						}
					}
				};
				
				channel.basicConsume(QUEUE_NAME2,false, consumer); //false�ر��Զ�Ӧ�𣬸�Ϊ�����ֶ�Ӧ��ʽ
			}

			
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
