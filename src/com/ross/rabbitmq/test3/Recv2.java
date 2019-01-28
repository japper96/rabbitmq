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
* 消费者场景：使用路由键过滤指定的日志消息（仅订阅过滤自己需要的消息）
* 信道指定交换名和类型（和生产者保持一致）
* 获取一个默认的队列名称
* 信道队列绑定交换器
* 订阅（等待消息）
*/
public class Recv2 {
	private final static String EXCHANGE_NAME = "logs"; //设置一个交换器名称
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
			
			//声明信道1，模拟同时两个信道接收消息
			Channel channel = connection.createChannel(); //创建信道			
			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");//指定交换器名称和类型，和生产者保持一致
			String queueName = channel.queueDeclare().getQueue(); //获取信道的队列默认名称
			channel.queueBind(queueName, EXCHANGE_NAME, "black"); //指定路由键为black			
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
			channel.basicConsume(queueName,true, consumer);//等待消息接收
			
			//声明信道2
			Channel channel2 = connection.createChannel(); //创建信道			
			channel2.exchangeDeclare(EXCHANGE_NAME, "fanout");//指定交换器名称和类型，和生产者保持一致
			String queueName2 = channel2.queueDeclare().getQueue(); //获取信道的队列默认名称
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
			channel2.basicConsume(queueName2,true, consumer2);//等待消息接收
			
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
