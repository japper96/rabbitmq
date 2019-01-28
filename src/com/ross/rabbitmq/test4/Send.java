package com.ross.rabbitmq.test4;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * ���������ģ��㲥��--3 ʹ��·�ɼ�����ָ������־��Ϣ�������Ĺ����Լ���Ҫ����Ϣ,ʹ��������ʽ��
 * ���ó����㣺
 * �ŵ����彻�������ͣ�topic���ͣ�������,������ʹ��topic���ͽ�����ʵ�������fanout���͵Ĺ㲥���ܡ�
 * �����߶Ͽ��������Զ�ɾ��
 * ����Ĭ�϶��У���Ϣ���͵�Ĭ�϶�����
 * ���ۣ���������߻�ͬʱ�յ���Ϣ
 * @author Administrator
 *
 */
public class Send {
	private final static String EXCHANGE_NAME = "topic_logs";	
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
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");//���彻�������ͺ�����
			
			//����5����Ϣ
			String[] logs = new  String[]{"message.debug","message.info","error.message","warrling.message"};
			for(int i =0;i<logs.length;i++)
			{
				String message = "a log message!!!"+logs[i];
				channel.basicPublish(EXCHANGE_NAME, logs[i], null,  new String(message.getBytes(),"UTF-8").getBytes());//ָ���������ͣ����͵�Ĭ��fanout���Ͷ���
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
