package com.rabbit.consumer.workqueues2;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Test {
	public static void main(String[] args) throws UnknownHostException {
		InetAddress[] addressArr = InetAddress.getAllByName("www.baidu.com");
		for(InetAddress a:addressArr)
			System.out.println(a);
	}
}
