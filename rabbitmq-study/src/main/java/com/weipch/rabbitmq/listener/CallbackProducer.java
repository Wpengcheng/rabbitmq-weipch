package com.weipch.rabbitmq.listener;

import com.rabbitmq.client.*;
import com.weipch.util.RabbitMqUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author 方唐镜
 * @Date 2025-03-19 9:47
 * @Description 生产者监听
 */
public class CallbackProducer {

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

        // 添加消息确认监听器
        channel.addConfirmListener(new ConfirmListener() {

            @Override
            public void handleAck(long l, boolean b) throws IOException {
                // 消息成功到达Broker时的回调
            }

            @Override
            public void handleNack(long l, boolean b) throws IOException {
                // 消息未到达Broker时的回调
            }
        });


        // 构建消息失败时监听器(路由失败触发)
        channel.addReturnListener(new ReturnListener() {
            /*
            1. int replyCode：表示返回的原因代码。
            2. String replyText：描述返回原因的文本。
            3. String exchange：消息被发送到的交换机名称。
            4. String routingKey：消息使用的路由键。
            5. AMQP.BasicProperties properties：消息的属性。
            6. byte[] body：消息体的字节数组
             */
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routeingKey, AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {
                System.out.println("return" + replyCode + "replyCode:" + replyCode + "exchange:" + exchange + "routeingKey:" + routeingKey + "bytes:" + new String(bytes));
            }
        });

        // 发送消息
        for (int i = 0; i < 4; i++){
            // 构建消息持久化属性
            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
            builder.deliveryMode(MessageProperties.PERSISTENT_TEXT_PLAIN.getDeliveryMode());
            builder.priority(MessageProperties.PERSISTENT_TEXT_PLAIN.getPriority());
            // 设置消息的关联ID，用于消息的回溯
            builder.correlationId(UUID.randomUUID().toString());
            String message = "callback-message" + i;
            channel.basicPublish(CALLBACK_EXCHANGE, "callback-routing-key", true, builder.build(), message.getBytes());
        }

        // 测试路由失败 addReturnListener
        channel.basicPublish(CALLBACK_EXCHANGE, "callback-routing-key1", true, null, "message".getBytes());
        // 不要立即关闭连接，否则生产者监听器不会收到消息
        Thread.sleep(10000);
        channel.close();

    }

}
