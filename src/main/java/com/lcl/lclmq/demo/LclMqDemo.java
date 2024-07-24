package com.lcl.lclmq.demo;

import com.alibaba.fastjson.JSON;
import com.lcl.lclmq.client.LclBroker;
import com.lcl.lclmq.client.LclConsumer;
import com.lcl.lclmq.model.LclMessage;
import com.lcl.lclmq.client.LclProducer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * @Author conglongli
 * @date 2024/7/11 21:17
 */
@Slf4j
public class LclMqDemo {
    public static void main(String[] args) throws IOException {
        long ids = 0;

        String topic = "com.lcl.test";

        LclBroker broker = LclBroker.Default;
//        broker.createTopic(topic);

        LclProducer producer = broker.createProducer();
//        LclConsumer<?> consumer = broker.createConsumer(topic);
//
//        consumer.listen(topic, message -> {
//            log.info("onMessage => {}", message);
//        });

        LclConsumer<?> consumer1 = broker.createConsumer(topic);


        for(int i=0; i<10; i++){
            Order order = new Order(ids, "item" + ids, 100*ids);
            producer.send(topic, new LclMessage<>((long)ids++, JSON.toJSONString(order), null));
        }

        for(int i=0; i<10; i++){
            LclMessage<String> message = (LclMessage<String>) consumer1.recv(topic);
            log.info("" + message);
            if(message != null){
                consumer1.ack(topic, message);
            }
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                consumer1.unsub(topic);
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100*ids);
                producer.send(topic, new LclMessage<>((long)ids++, JSON.toJSONString(order), null));
                log.info("produce ok =>>> " + order);
            }
            if (c == 'c') {
                LclMessage<String> message = (LclMessage<String>) consumer1.recv(topic);
                if(message != null){
                    consumer1.ack(topic, message);
                }
                log.info("consume ok =>>> {}", message);
            }
            if (c == 'a') {
                for(int i=0; i<10; i++){
                    Order order = new Order(ids, "item" + ids, 100*ids);
                    producer.send(topic, new LclMessage<>((long)ids++, JSON.toJSONString(order), null));
                }
                log.info("produce 10 orders ...  ok");
            }
        }
    }
}
