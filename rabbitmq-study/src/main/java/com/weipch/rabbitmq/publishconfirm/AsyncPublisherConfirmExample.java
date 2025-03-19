
package com.weipch.rabbitmq.publishconfirm;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.weipch.util.RabbitMqUtils;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 异步发布确认示例
 * 异步确认是一种更灵活的确认方式，通过回调函数处理消息的确认和未确认事件。这种方式可以在异步场景中更好地处理消息的状态。
 */

public class AsyncPublisherConfirmExample {

    private static final String QUEUE_NAME = "my_queue";
    private static final int MESSAGE_COUNT = 100;

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        // 声明持久化队列（服务重启后消息不丢失）
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        //开启发布确认
        channel.confirmSelect();

        /**
         * outstandingConfirms：线程安全有序（默认是升序）的一个哈希表，用于存储未被确认的消息，适用于高并发的情况
         * 1.轻松的将序号与消息进行关联（key：消息序号，value：消息内容）
         * 2.轻松批量删除条目 只要给到序列号
         * 3.支持并发访问
         */
        ConcurrentSkipListMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();
        /**
         * 确认收到消息的一个回调
         * 参数1.消息序列号
         * 参数2.true（批量确认） 可以确认小于等于当前序列号的消息
         *      false（单个确认） 确认当前序列号消息
         */
        ConfirmCallback ackCallback = (sequenceNumber, multiple) -> {
            if (multiple) {
                //  true：返回小于等于当前序列号sequenceNumber的所有消息；false：返回所有小于 sequenceNumber的所有消息
                //  执行完得到一个确认消息的视图map然后将其clear就是将其从outstandingConfirms中删除，因为这个视图是基于原Map的，它们共享同一份数据。因此，对视图的修改会直接反映到原Map中。
                //  这样outstandingConfirms中就只剩下未确认的消息。
                ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                //批量清除该部分未确认消息
                confirmed.clear();
            } else {
                //只清除当前序列号的消息
                outstandingConfirms.remove(sequenceNumber);
            }
        };
        /**
         * 未确认消息回调，
         */
        ConfirmCallback nackCallback = (sequenceNumber, multiple) -> {
            String message = outstandingConfirms.get(sequenceNumber);
            System.out.println("发布的消息" + message + "未被确认，序列号" + sequenceNumber);
        };
        /**
         * 添加一个异步确认的监听器
         * 1.确认收到消息的回调
         * 2.未收到消息的回调
         */
        channel.addConfirmListener(ackCallback, nackCallback);
        long begin = System.currentTimeMillis();
        //发送消息
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = "消息" + i;
            /**
             * channel.getNextPublishSeqNo()获取下一个消息的序列号
             * 通过序列号与消息体进行一个关联
             * 全部都是未确认的消息体
             */
            outstandingConfirms.put(channel.getNextPublishSeqNo(), message);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + MESSAGE_COUNT + "个异步确认消息,耗时" + (end - begin) + "ms");
    }

}