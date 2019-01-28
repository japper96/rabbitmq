package com.ross.rabbitmq.test1;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * 点对点：一次只发送一条消息到worker，非广播式的发布/订阅功能
 * 测试基本的消息发送：含队列和消息的持久化和非持久化、多个消费者按序接收并处理、设置消费者每次处理完毕后再接收消息
 * 手动消息确认boolean autoAck = true ;设置每次只接收一条消息（确认处理完毕后再接收）channel.basicQos（prefetchCount）;
 * 消息持久化：channel.queueDeclare("task_queue",true,false,false,null);
 * @author Administrator
 *
 */
public class Send {
	private final static String QUEUE_NAME1 = "Hello!"; //设置一个队列名字
	private final static String QUEUE_NAME2 = "Hello!durable"; //设置一个队列名字,队列消息持久化，即使重启也不会丢失
	
	public static void main(String[] args) {
		ConnectionFactory factory = new ConnectionFactory(); //初始化连接工厂
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");
		factory.setPassword("0000");
		Connection connection = null ;
		Channel channel = null;
		try {
			connection = factory.newConnection(); //创建连接
			channel = connection.createChannel(); //创建信道
			boolean durable = true; //持久化消息队列（生产者和消费者同时修改，如果队列已经存在则修改参数无效，可以另外新建立一个队列名）			
			channel.queueDeclare(QUEUE_NAME2, durable, false, false, null);//声明队列、如果不存在则创建
			
			//发送5次消息
			for(int i =0;i<50;i++)
			{
				String[] messages = new String[]{"daya"+i,"ordera"+i,"dayb"+i,"orderb"+i,"dayc"+i,"orderc"+i};//需要发送的内容
				String message = getMessage(messages);
				BasicProperties durableMessage = MessageProperties.PERSISTENT_TEXT_PLAIN;//消息持久化（上面是消息队列持久化）
				channel.basicPublish("", QUEUE_NAME2, durableMessage, new String(message.getBytes(),"UTF-8").getBytes());//推送到队列	
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				System.out.println("sent ...'"+message+"'");
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
			if(channel != null)
			{
					try {
						channel.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TimeoutException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			if(connection != null)
			{
				try {
					connection.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	private static String getMessage(String[] strings)
	{
		if(strings.length<1)
		{
			return "Hello World!!!";
		}
		return joinStrings(strings," ");
	}
	
	private  static String joinStrings(String[] strings,String delimiter)
	{
		int length=strings.length;
		if(length == 0) return "";
		StringBuilder words = new StringBuilder(strings[0]);
		for(int i= 1;i<length;i++)
		{
			words.append(delimiter).append(strings[i]);
		}
		return words.toString();
	}
}
