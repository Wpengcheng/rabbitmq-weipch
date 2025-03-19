package com.weipch.rabbitmq.publishconfirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.weipch.util.RabbitMqUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;

public class BatchPublisherConfirmExample {
	private static final String QUEUE_NAME = "my_queue";

	public static void main(String[] args) throws Exception {

		   Channel channel = RabbitMqUtils.getChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);

			// 开启发布确认
			channel.confirmSelect();

			// 发布多条消息
			for (int i = 0; i < 100; i++) {
				String message = "Message " + i;
				channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
			}

			// 批量确认
			if (confirmMessages(channel)) {
				System.out.println("All messages sent successfully.");
			} else {
				System.out.println("Failed to send one or more messages.");
			}

	}

	private static boolean confirmMessages(Channel channel) throws InterruptedException, TimeoutException {
		// 批量确认消息的序号
		final SortedSet<Long> unconfirmedSet = Collections.synchronizedSortedSet(new TreeSet<>());

		// 添加确认监听器
		channel.addConfirmListener(new ConfirmListener() {
			@Override
			public void handleAck(long deliveryTag, boolean multiple) throws IOException {
				if (multiple) {
					// 批量确认时，将小于等于 deliveryTag 的所有消息标记为已确认
					unconfirmedSet.headSet(deliveryTag + 1).clear();
				} else {
					// 单个确认时，直接移除该消息标记
					unconfirmedSet.remove(deliveryTag);
				}
			}

			@Override
			public void handleNack(long deliveryTag, boolean multiple) throws IOException {
				// 处理未确认的消息，可选择重发或进行其他处理
				System.out.println("Message with delivery tag " + deliveryTag + " not confirmed.");
			}
		});

		// 等待服务器确认，设置超时时间为5000毫秒
		if (channel.waitForConfirms(5000)) {
			return true;
		} else {
			// 处理未确认的消息，可选择重发或进行其他处理
			System.out.println("Timeout: Some messages not confirmed.");
			return false;
		}
	}
}