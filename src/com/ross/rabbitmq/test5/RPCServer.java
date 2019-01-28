package com.ross.rabbitmq.test5;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


/**
 * Զ�̵���(RPC)--4 ����ˣ�ʹ����������ݣ�
 * �����߳�����
 * ����˶��ĵȴ��������������
 * ����RPC����ʹ��replyTo�ֶν�������ؿͻ���
 * �ͻ����ڻص������еȴ����ݣ�����Ϣ���ؼ��correlationId����ƥ��ֵ���ظ�������Ӧ
 * 
 * Ŀǰ�������Ψһ��RPCʵ�ַ������������ַ����и��ŵ㣺���һ��RPC������Ӧ̫������ô�������ж༸������ͬʱ����
 * @author Administrator
 *
 */
public class RPCServer {
	private final static String RPC_QUEUE_NAME = "rpc_queue"; //����һ����������
	public static void main(String[] args) {
		ConnectionFactory factory = new ConnectionFactory(); //��ʼ�����ӹ���
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");
		factory.setPassword("0000");
		Connection connection = null ;
		
		try {
			connection = factory.newConnection(); //��������
			final Channel channel = connection.createChannel(); //�����ŵ�
			
			channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null);
			channel.basicQos(1);//ÿ��ֻ����һ����Ϣ�������ڶ���������Ͻ��з��为��
			
			System.out.println("rpcserver wait message ...");
			
			//�����߶��壬���лص�����
			final Consumer consumer = new DefaultConsumer(channel){
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
						byte[] body) throws IOException {
					
					BasicProperties replyProps = new BasicProperties()
							.builder()
							.correlationId(properties.getCorrelationId())
							.build();
					
					String response = "";
					
					try
					{
						String message = new String(body,"UTF-8");
						System.out.println("rpcserver[.] fib ("+message+")");
						
						int n = Integer.parseInt(message);//�����ǻ�ȡ���͹�����RPC�������֣���1,2,3,4...
						//����ʵ��ҵ���߼������������
						switch (n)
						{
							case 0:
								response += "method result0:"+n;
								break;
							case 1:
								response = "method result1:"+n;
								break;
							case 2:
								response = "method result2:"+n;
								break;
							case 3:
								response = "method result3:"+n;
								break;
							case 4:
								response = "method result4:"+n;
								break;
							default:
								response = "unknown method!!!";
								break;
						}
					}finally
					{
						channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));//�ص�����
						channel.basicAck(envelope.getDeliveryTag(), false);
						//������֪ͨ
						synchronized(this)
						{
							this.notify();
						}
					}
				}
			};
			
			//�����߶���
			channel.basicConsume(RPC_QUEUE_NAME, false,consumer);
			while(true)
			{
				//�ȴ���׼��������RPC�ͻ��˵���Ϣ
				synchronized(consumer)
				{
					try {
						consumer.wait();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
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
			//�ر�����
			if(connection!=null)
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
}
