package com.lcl.lclmq.server;

import com.lcl.lclmq.model.LclMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author conglongli
 * @date 2024/7/13 22:15
 */
@Slf4j
public class MessageQueue {

    public static final Map<String, MessageQueue> queues = new HashMap<>();
    private static final String TEST_TOPIC = "com.lcl.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();


    private String topic;
    private LclMessage<?>[] queue = new LclMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic){
        this.topic = topic;
    }

    public int send(LclMessage<?> message) {
        if(index >= queue.length){
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public LclMessage<?> recv(int recvIndex){
        if(recvIndex >=0 && recvIndex <= index){
            return queue[recvIndex];
        }
        return null;
    }

    public void subscribe(MessageSubscription subscription){
        subscriptions.putIfAbsent(subscription.getConsumerId(), subscription);
    }

    public static void sub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if(messageQueue == null){
            return;
        }
        messageQueue.unSubscribe(subscription);
    }

    private void unSubscribe(MessageSubscription subscription) {
        subscriptions.remove(subscription.getConsumerId());
    }

    public static int send(String topic, String consumerId, LclMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        return messageQueue.send(message);
    }

    /**
     * 使用此方法，需要手动调用 ack 手动更新 offset
     * @param topic
     * @param consumerId
     * @return
     */
    public static LclMessage<?> recv(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        if(!messageQueue.subscriptions.containsKey(consumerId)){
            throw new RuntimeException("subscription not found for topic/consumerId：" + topic + "/" + consumerId);
        }
        int recvIndex = messageQueue.subscriptions.get(consumerId).getOffset();
        return messageQueue.recv(recvIndex);
    }

    public static int ack(String topic, String consumerId, Integer offset){
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        if(!messageQueue.subscriptions.containsKey(consumerId)){
            throw new RuntimeException("subscription not found for topic/consumerId：" + topic + "/" + consumerId);
        }
        MessageSubscription messageSubscription = messageQueue.subscriptions.get(consumerId);
        if(offset <= messageSubscription.getOffset() || offset >= messageQueue.index){
            log.error("offset illegality：" + offset);
            return -1;
        }
        messageSubscription.setOffset(offset);
        return offset;
    }
}
