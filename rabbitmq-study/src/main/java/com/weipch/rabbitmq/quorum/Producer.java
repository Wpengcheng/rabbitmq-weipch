package com.weipch.rabbitmq.quorum;

import com.rabbitmq.client.Channel;
import com.weipch.util.RabbitMqUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author 方唐镜
 * @Date 2025-03-19 19:58
 * @Description
 */
public class Producer {

    private static final String QUEUE_NAME = "quorum_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();

        // 定义队列类型为quorum
        channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of("x-queue-type", "quorum"));

        String message = "quorum-message";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
    }

}
