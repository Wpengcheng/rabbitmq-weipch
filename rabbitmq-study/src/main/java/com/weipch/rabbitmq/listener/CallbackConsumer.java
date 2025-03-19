package com.weipch.rabbitmq.listener;

import com.rabbitmq.client.*;
import com.weipch.util.RabbitMqUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author 方唐镜
 * @Date 2025-03-19 9:53
 * @Description
 */
public class CallbackConsumer {

    private static final String CALLBACK_EXCHANGE = "callback_exchange";
    private static final String ALTER_EXCHANGE = "alter_exchange";
    private static final String CALLBACK_QUEUE = "callback_queue";


    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        Map<String, Object> params = new HashMap<>();
        params.put("alternate-exchange", ALTER_EXCHANGE);
        channel.exchangeDeclare(CALLBACK_EXCHANGE, BuiltinExchangeType.DIRECT, true, false, params);
        channel.exchangeDeclare(ALTER_EXCHANGE, BuiltinExchangeType.DIRECT, true, false, null);

        channel.queueDeclare(CALLBACK_QUEUE, true, false, false, null);

        channel.queueBind(CALLBACK_QUEUE, CALLBACK_EXCHANGE, "callback-routing-key");

        channel.basicConsume(CALLBACK_QUEUE,
                // 处理消息，并通过basicAck手动确认，确保消息被正确处理后才从队列移除。
                new DeliverCallback() {
                    @Override
                    public void handle(String consumerTag, Delivery delivery) throws IOException {
                        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                        String correlationId = delivery.getProperties().getCorrelationId();
                        System.out.println("DeliverCallback consumerTag:" + consumerTag + ", delivery:"+ delivery + ", deliveryTag:" + deliveryTag + ", correlationId:" + correlationId);
                        channel.basicAck(deliveryTag, false);
                    }
                },
                // 监听消费者意外取消的情况（如队列被删除）
                new CancelCallback() {
                    @Override
                    public void handle(String consumerTag) throws IOException {
                        System.out.println("CancelCallback consumerTag:" + consumerTag);
                    }
                },
                // 监听通道或连接关闭的情况
                new ConsumerShutdownSignalCallback() {
                    @Override
                    public void handleShutdownSignal(String consumerTag, ShutdownSignalException e) {
                        System.out.println("ConsumerShutdownSignalCallback consumerTag:" + consumerTag + ", e:" + e);
                    }
                }

        );

    }

}
