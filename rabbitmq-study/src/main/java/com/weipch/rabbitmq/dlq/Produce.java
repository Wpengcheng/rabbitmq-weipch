package com.weipch.rabbitmq.dlq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.weipch.util.RabbitMqUtils;

/**
 * @Author 方唐镜
 * @Create 2024-03-03 14:08
 * @Description
 */
public class Produce {


	private static final String NORMAL_EXCHANGE = "normal_exchange";


	public static void main(String[] args) throws Exception {
		Channel channel = RabbitMqUtils.getChannel();
		AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().expiration("10000").build();
		for (int i = 0; i < 10; i++) {
			String message = "hello world" + i;
			channel.basicPublish(NORMAL_EXCHANGE, "normal-routing-key", properties, message.getBytes());
		}

	}
}
