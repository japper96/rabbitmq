package com.ross.rabbitmq.test1;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

/**
 * ��Ե㣺һ��ֻ����һ����Ϣ��worker���ǹ㲥ʽ�ķ���/���Ĺ���
 * ���Ի�������Ϣ���ͣ������к���Ϣ�ĳ־û��ͷǳ־û�����������߰�����ղ���������������ÿ�δ�����Ϻ��ٽ�����Ϣ
 * �ֶ���Ϣȷ��boolean autoAck = true ;����ÿ��ֻ����һ����Ϣ��ȷ�ϴ�����Ϻ��ٽ��գ�channel.basicQos��prefetchCount��;
 * ��Ϣ�־û���channel.queueDeclare("task_queue",true,false,false,null);
 * @author Administrator
 *
 */
public class Send {
	private final static String QUEUE_NAME1 = "Hello!"; //����һ����������
	private final static String QUEUE_NAME2 = "Hello!durable"; //����һ����������,������Ϣ�־û�����ʹ����Ҳ���ᶪʧ
	
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
			boolean durable = true; //�־û���Ϣ���У������ߺ�������ͬʱ�޸ģ���������Ѿ��������޸Ĳ�����Ч�����������½���һ����������			
			channel.queueDeclare(QUEUE_NAME2, durable, false, false, null);//�������С�����������򴴽�
			
			//����5����Ϣ
			for(int i =0;i<50;i++)
			{
				String[] messages = new String[]{"daya"+i,"ordera"+i,"dayb"+i,"orderb"+i,"dayc"+i,"orderc"+i};//��Ҫ���͵�����
				String message = getMessage(messages);
				BasicProperties durableMessage = MessageProperties.PERSISTENT_TEXT_PLAIN;//��Ϣ�־û�����������Ϣ���г־û���
				channel.basicPublish("", QUEUE_NAME2, durableMessage, new String(message.getBytes(),"UTF-8").getBytes());//���͵�����	
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
