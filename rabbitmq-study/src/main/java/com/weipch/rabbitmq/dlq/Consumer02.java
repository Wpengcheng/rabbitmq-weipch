package com.weipch.rabbitmq.dlq;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.weipch.util.RabbitMqUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author 方唐镜
 * @Create 2024-03-03 13:50
 * @Description
 */
public class Consumer02 {

	private static final String DEAD_QUEUE = "dead_queue";


	public static void main(String[] args) throws Exception {
		Channel channel = RabbitMqUtils.getChannel();
		channel.basicConsume(DEAD_QUEUE, true,
			(consumerTag, delivery) -> System.out.println("Consumer02:" + new String(delivery.getBody(), StandardCharsets.UTF_8)),
			(consumerTag, e) -> {});
	}
}