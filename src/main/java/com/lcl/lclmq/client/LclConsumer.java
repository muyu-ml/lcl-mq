package com.lcl.lclmq.client;

import com.lcl.lclmq.model.LclMessage;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author conglongli
 * @date 2024/7/11 21:11
 */
public class LclConsumer<T> {

    private String id;
    private LclBroker broker;
//    private String topic;
//    LclMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public LclConsumer(LclBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

//    public void subscribe(String topic) {
//        broker.sub(topic, id);
//    }

    public LclMessage<T> recv(String topic) {
        return broker.recv(topic, id);
    }

    public void sub(String topic) {
        broker.sub(topic, id);
    }

    public void unsub(String topic) {
        broker.unsub(topic, id);
    }

    public boolean ack(String topic, LclMessage<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }

    public void listen(String topic, LclListener<T> listener) {
        this.listener = listener;
        broker.addListen(topic, this);
    }

    @Getter
    private LclListener<T> listener;

}
