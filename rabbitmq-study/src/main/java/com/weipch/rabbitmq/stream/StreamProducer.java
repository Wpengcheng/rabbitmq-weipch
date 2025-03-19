package com.weipch.rabbitmq.stream;

import com.rabbitmq.client.Channel;
import com.weipch.util.RabbitMqUtils;

import java.util.Map;

/**
 * @Author 方唐镜
 * @Date 2025-03-19 19:58
 * @Description
 */
public class StreamProducer {

    private static final String QUEUE_NAME = "stream_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();

        // 定义队列类型为quorum
        channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of("x-queue-type", "stream"));

        String message = "stream-message";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
    }

}
