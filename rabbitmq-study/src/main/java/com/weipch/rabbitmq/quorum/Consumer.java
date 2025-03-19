package com.weipch.rabbitmq.quorum;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.weipch.util.RabbitMqUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author 方唐镜
 * @Date 2025-03-19 19:52
 * @Description
 */
public class Consumer {

    private static final String QUEUE_NAME = "quorum_queue";


    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();

        // 定义队列类型为quorum
        channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of("x-queue-type", "quorum"));


        channel.basicConsume(QUEUE_NAME, true, new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("handleDelivery consumerTag:" + consumerTag + ", deliveryTag:" + envelope.getDeliveryTag() + ", contentType:" + properties.getContentType() + ", body:" + StandardCharsets.UTF_8.decode(ByteBuffer.wrap(body)));
            }
        });
    }

}
