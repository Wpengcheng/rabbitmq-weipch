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
public class Consumer01 {


	private static final String NORMAL_EXCHANGE = "normal_exchange";
	private static final String DEAD_EXCHANGE = "dead_exchange";

	private static final String NORMAL_QUEUE = "normal_queue";
	private static final String DEAD_QUEUE = "dead_queue";


	public static void main(String[] args) throws Exception {
		Channel channel = RabbitMqUtils.getChannel();
		//声明死信交换机和队列
		channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);
		channel.queueDeclare(DEAD_QUEUE, false, false, false, null);
		//绑定
		channel.queueBind(DEAD_QUEUE, DEAD_EXCHANGE, "dead-routing-key");


		//声明普通交换机和队列
		channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
		//死信配制 指定死信交换机和死信路由键
		Map<String, Object> map = new HashMap<>();
		map.put("x-dead-letter-exchange", DEAD_EXCHANGE);
		map.put("x-dead-letter-routing-key", "dead-routing-key");
		channel.queueDeclare(NORMAL_QUEUE, false, false, false, map);
		channel.queueBind(NORMAL_QUEUE, NORMAL_EXCHANGE, "normal-routing-key");
		channel.basicConsume(NORMAL_QUEUE, true,
			(consumerTag, delivery) -> System.out.println("Consumer01:" + new String(delivery.getBody(), StandardCharsets.UTF_8)),
			(consumerTag, e) -> {});
	}
}