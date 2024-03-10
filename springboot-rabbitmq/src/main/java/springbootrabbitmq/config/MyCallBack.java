package springbootrabbitmq.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.events.Event;

import javax.annotation.PostConstruct;
import java.util.function.LongFunction;


@Slf4j
@Component
public class MyCallBack implements RabbitTemplate.ConfirmCallback,RabbitTemplate.ReturnsCallback {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @PostConstruct
    public void init() {
        //    注入
        rabbitTemplate.setConfirmCallback(this);
        rabbitTemplate.setReturnsCallback(this);
    }

    /*
     * 交换机确认回调方法
     * 1.发消息 交换机接收到了 回调
     *    1.1 correlationData 保存回调消息的ID及相关信息
     *    1.2 交换机收到消息 ture
     *    1.3 cause null
     * 2.发消息 交换机接收失败了 回调
     *    2.1 correlationData 保存回调消息的ID及相关消息
     *    2.2 交换机收到消息 false
     *    2.3 cause 失败的原因
     */
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        String id = correlationData != null ? correlationData.getId() : "";
        if (ack) {
            log.info("交换机已经收到ID为：{}", id);
        } else {
            log.info("交换机还未收到ID为：{}的消息，由于原因：{}", id, cause);
        }
    }


    //    数可以在当消息传递过程中不可达目的地时将消息返回给生产者
    //    只有不可大目的地的时候，才进行回退
    @Override
    public void returnedMessage(ReturnedMessage returned) {
        log.info("消息{}，被交换机{}退回，退回的原因：{}，路由Key:{}",returned.getMessage(),returned.getExchange(),returned.getReplyText(),returned.getRoutingKey());
    }
}
