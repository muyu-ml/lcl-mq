package com.lcl.lclmq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics
 * @Author conglongli
 * @date 2024/7/11 20:59
 */
public class LclBroker {
    Map<String, LclMq> mqMapping = new ConcurrentHashMap<>(64);

    public LclMq find(String topic) {
        return mqMapping.get(topic);
    }

    public LclMq createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new LclMq(topic));
    }

    public LclProducer createProducer(){
        return new LclProducer(this);
    }

    public LclConsumer<?> createConsumer(String topic) {
        LclConsumer<?> consumer = new LclConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }
}
