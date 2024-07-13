package com.lcl.lclmq.client;

import com.lcl.lclmq.model.LclMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author conglongli
 * @date 2024/7/11 21:11
 */
public class LclConsumer<T> {

    private String id;
    private LclBroker broker;
    private String topic;
    LclMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public LclConsumer(LclBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        mq = broker.find(topic);
        if(mq == null) {
            throw new RuntimeException("topic not find");
        }
    }

    public LclMessage<T> poll(long timeout) {
        return mq.poll(timeout);
    }

    public void listen(LclListener<T> listener) {
        mq.addListen(listener);
    }
}
