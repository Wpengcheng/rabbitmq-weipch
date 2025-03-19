package com.weipch.rabbitmq.stream;

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
public class StreamConsumer {

    private static final String QUEUE_NAME = "stream_queue";


    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();

        // 定义队列类型为stream
        channel.queueDeclare(QUEUE_NAME, true, false, false, Map.of("x-queue-type", "stream"));


        /**
         * x-stream-offset参数：指定消费者开始消费的位置的
         * first：从队列中第一条消息开始消费
         * last：从队列中最后一条消息开始消费
         * offset：从指定位置开始消费
         * next：从队列中下一条消息开始消费
         * timestamp：从指定时间戳开始消费
         */
        Map<String, Object> params = new HashMap<>();
        params.put("x-stream-offset", "next");
        channel.basicConsume(QUEUE_NAME, true, params, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("handleDelivery consumerTag:" + consumerTag + ", deliveryTag:" + envelope.getDeliveryTag() + ", contentType:" + properties.getContentType() + ", body:" + StandardCharsets.UTF_8.decode(ByteBuffer.wrap(body)));
            }
        });
    }

}
