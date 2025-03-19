package com.weipch.rabbitmq.publishconfirm;

import com.rabbitmq.client.*;
import com.weipch.util.RabbitMqUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @Author 方唐镜
 * @Date 2025-03-19 14:21
 * @Description
 */
public class ConfirmConsumer {

    private static final String QUEUE_NAME = "my_queue";
    private static int offset = 0;

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.basicConsume(QUEUE_NAME, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("====================");
                System.out.println("handleDelivery consumerTag:" + consumerTag + ", deliveryTag:" + envelope.getDeliveryTag() + ", contentType:" + properties.getContentType() + ", body:" +StandardCharsets.UTF_8.decode(ByteBuffer.wrap(body)));
//                if (offset++ % 2 == 0){
                    channel.basicAck(envelope.getDeliveryTag(), true);
//                }else {
//                    channel.basicNack(envelope.getDeliveryTag(), true, true);
//                }
            }
        });




    }
}