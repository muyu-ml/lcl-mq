package com.lcl.lclmq.client;

import com.lcl.lclmq.model.LclMessage;
import lombok.AllArgsConstructor;

/**
 * @Author conglongli
 * @date 2024/7/11 20:58
 */
@AllArgsConstructor
public class LclProducer {

    private LclBroker broker;
    public boolean send(String topic, LclMessage message) {
        LclMq mq = broker.find(topic);
        if(mq == null) {
            throw new RuntimeException("topic not find");
        }
        return mq.send(message);
    }
}
