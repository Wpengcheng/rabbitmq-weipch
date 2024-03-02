package com.weipch.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AsyncPublisherConfirmExample {
	private static final String QUEUE_NAME = "my_queue";

	public static void main(String[] args) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		try (Connection connection = factory.newConnection();
		     Channel channel = connection.createChannel()) {

			channel.queueDeclare(QUEUE_NAME, false, false, false, null);

			// 开启异步发布确认模式
			channel.confirmSelect();

			// 发布消息
			for (int i = 0; i < 100; i++) {
				String message = "Message " + i;
				channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
			}

			// 设置异步确认的回调函数
			setAsyncConfirmCallback(channel);
		}
	}

	private static void setAsyncConfirmCallback(Channel channel) {
		// 使用线程安全的跳表存储未确认消息的序号
		final ConcurrentNavigableMap<Long, Boolean> unconfirmedSet = new ConcurrentSkipListMap<>();
		
		// 异步发布确认的回调函数
		ConfirmCallback ackconfirmCallback = (deliveryTag, multiple) -> {
			if (multiple) {
				// 批量确认时，删除已确认的消息
				unconfirmedSet.headMap(deliveryTag + 1).clear();
			} else {
				// 单个确认时，直接移除该消息标记
				unconfirmedSet.remove(deliveryTag);
			}
			System.out.println("Message with delivery tag " + deliveryTag + " confirmed.");
		};
		
		// 异步发布未确认的回调函数
		ConfirmCallback nackconfirmCallback = (deliveryTag, multiple) -> {
			// 将消息的序号标记为未确认
			unconfirmedSet.put(deliveryTag, false);
			System.out.println("Message with delivery tag " + deliveryTag + " sent.");
		};
		channel.addConfirmListener(ackconfirmCallback,nackconfirmCallback);
	}
}