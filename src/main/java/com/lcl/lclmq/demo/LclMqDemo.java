package com.lcl.lclmq.demo;

import com.lcl.lclmq.client.LclBroker;
import com.lcl.lclmq.client.LclConsumer;
import com.lcl.lclmq.model.LclMessage;
import com.lcl.lclmq.client.LclProducer;
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

        String topic = "lcl.order";
        LclBroker broker = new LclBroker();
        broker.createTopic(topic);

        LclProducer producer = broker.createProducer();
        LclConsumer<?> consumer = broker.createConsumer(topic);

        consumer.listen(message -> {
            log.info("onMessage => {}", message);
        });


        for(int i=0; i<10; i++){
            Order order = new Order(ids, "item" + ids, 100*ids);
            producer.send(topic, new LclMessage<>((long)ids++, order, null, null));
        }

        for(int i=0; i<10; i++){
            LclMessage<Order> message = (LclMessage<Order>) consumer.poll(1000);
            log.info("" + message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                Order order = new Order(ids, "item" + ids, 100*ids);
                producer.send(topic, new LclMessage<>((long)ids++, order, null, null));
                log.info("send ok =>>> " + order);
            }
            if (c == 'c') {
                LclMessage<Order> message = (LclMessage<Order>) consumer.poll(1000);
                log.info("poll ok =>>> {}", message);
            }
            if (c == 'a') {
                for(int i=0; i<10; i++){
                    Order order = new Order(ids, "item" + ids, 100*ids);
                    producer.send(topic, new LclMessage<>((long)ids++, order, null, null));
                }
                log.info("send 10 orders ...  ok");
            }
        }
    }
}
