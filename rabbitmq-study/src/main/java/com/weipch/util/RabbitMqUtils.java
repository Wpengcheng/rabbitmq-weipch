package com.weipch.util;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class RabbitMqUtils {
    //得到一个连接的 channel
    public static Channel getChannel() throws Exception {
        //创建一个连接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.181.130");
        factory.setUsername("admin");
        factory.setPassword("123");
        factory.setPort(5672);
        Connection connection = factory.newConnection();
        return connection.createChannel();
    }
}