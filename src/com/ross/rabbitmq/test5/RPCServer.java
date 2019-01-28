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
 * 远程调用(RPC)--4 服务端（使用消费者身份）
 * 消费者场景：
 * 服务端订阅等待队列上面的请求
 * 接收RPC请求并使用replyTo字段将结果返回客户端
 * 客户端在回调队列中等待数据，有消息返回检测correlationId属性匹配值返回给程序响应
 * 
 * 目前这个不是唯一的RPC实现方法，不过这种方法有个优点：如果一个RPC服务响应太慢，那么可以运行多几个服务同时处理。
 * @author Administrator
 *
 */
public class RPCServer {
	private final static String RPC_QUEUE_NAME = "rpc_queue"; //设置一个队列名称
	public static void main(String[] args) {
		ConnectionFactory factory = new ConnectionFactory(); //初始化连接工厂
		factory.setHost("127.0.0.1");
		factory.setPort(5672);
		factory.setUsername("ross");
		factory.setPassword("0000");
		Connection connection = null ;
		
		try {
			connection = factory.newConnection(); //创建连接
			final Channel channel = connection.createChannel(); //创建信道
			
			channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null);
			channel.basicQos(1);//每次只处理一个消息，可以在多个服务器上进行分配负载
			
			System.out.println("rpcserver wait message ...");
			
			//消费者定义，带有回调函数
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
						
						int n = Integer.parseInt(message);//这里是获取发送过来的RPC函数数字，如1,2,3,4...
						//根据实际业务逻辑返回相关数据
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
						channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));//回调函数
						channel.basicAck(envelope.getDeliveryTag(), false);
						//消费者通知
						synchronized(this)
						{
							this.notify();
						}
					}
				}
			};
			
			//消费者订阅
			channel.basicConsume(RPC_QUEUE_NAME, false,consumer);
			while(true)
			{
				//等待并准备好来自RPC客户端的消息
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
			//关闭连接
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
