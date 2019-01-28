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
	private final static String QUEUE_NAME1 = "Hello!"; //设置一个队列名字
	private final static String QUEUE_NAME2 = "Hello!durable"; //设置一个队列名字,队列消息持久化，即使重启也不会丢失
	public static void main(String[] args) {
		ConnectionFactory factory = new ConnectionFactory(); //初始化连接工厂
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");
		factory.setPassword("0000");
		Connection connection = null ;
//		Channel channel = null;
		
		try {
			connection = factory.newConnection(); //创建连接
			for (int i = 0 ;i<5;i++)
			{
				final Channel channel = connection.createChannel(); //创建信道
				boolean durable = true; //持久化消息队列（生产者和消费者同时修改，如果队列已经存在则修改参数无效，可以另外新建立一个队列名）			
				channel.queueDeclare(QUEUE_NAME2, durable, false, false, null);//声明队列、如果不存在则创建
				int prefetch = 1;
				channel.basicQos(prefetch);//声明一次只接受一个消息，直到处理完毕这条消息再接收。
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
							doWork(message); //模拟消息处理过程，处理完毕后才能进行响应
						}finally
						{
							System.out.println("Recv3 [x] Done");
							channel.basicAck(envelope.getDeliveryTag(), false); //处理完消息，返回应答状态
						}
					}
				};
				
				channel.basicConsume(QUEUE_NAME2,false, consumer); //false关闭自动应答，改为如上手动应答方式
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
