package com.lcl.lclmq.core;

/**
 * @Author conglongli
 * @date 2024/7/11 21:11
 */
public class LclConsumer<T> {

    private LclBroker broker;
    private String topic;
    LclMq mq;

    public LclConsumer(LclBroker broker) {
        this.broker = broker;
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
