package com.ross.rabbitmq.test5;

public class RPCTest {

	public static void main(String[] args) {
		try {
			for(int i =0;i<30;i++)
			{
				RPCClient rpcclient = new RPCClient();
				System.out.println("rpcclient[x] Request fib "+i);
				String response = rpcclient.call(String.valueOf(i));
				System.out.println("rpcclient[.] Got '" + response +"'");
				rpcclient.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		finally
		{
			
		}

	}

}
