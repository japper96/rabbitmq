package com.ross.rabbitmq.test3;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * 发布、订阅（广播）--2 使用路由键过滤指定的日志消息（仅订阅过滤自己需要的消息）
 * 设置场景点：
 * 信道定义交换器类型（direct类型）和名称,这里是使用direct类型交换器实现了替代fanout类型的广播功能。
 * 消费者断开，队列自动删除
 * 采用默认队列，消息推送到默认队列中
 * 结论：多个消费者会同时收到消息
 * @author Administrator
 *
 */
public class Send {
//	private final static String QUEUE_NAME1 = "Hello!"; //设置一个队列名字
//	private final static String QUEUE_NAME2 = "Hello!durable"; //设置一个队列名字,队列消息持久化，即使重启也不会丢失
	private final static String EXCHANGE_NAME = "direct_logs";
	
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
//			channel.exchangeDeclare(EXCHANGE_NAME, "fanout");//定义交换器类型和名称
			channel.exchangeDeclare(EXCHANGE_NAME, "direct");//定义交换器类型和名称
			
			//发送5次消息
			String[] logs = new  String[]{"debug","info","error","warrling"};
			for(int i =0;i<logs.length;i++)
			{
				String message = "a log message!!!"+logs[i];
				channel.basicPublish(EXCHANGE_NAME, logs[i], null,  new String(message.getBytes(),"UTF-8").getBytes());//指定交换类型，推送到默认fanout类型队列
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
