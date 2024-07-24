package com.lcl.lclmq.server;

import com.lcl.lclmq.model.LclMessage;
import com.lcl.lclmq.store.Indexer;
import com.lcl.lclmq.store.Store;
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

    // ConsumerId -> MessageSubscription
    private Map<String, MessageSubscription> subscriptions = new HashMap<>();


    private String topic;
//    private LclMessage<?>[] queue = new LclMessage[1024 * 10];
//    private int index = 0;

    private Store store = null;

    public MessageQueue(String topic){
        this.topic = topic;
        store = new Store(topic);
        store.init();
    }


    public int send(LclMessage<String> message) {
        if(message.getHeaders() == null){
            message.setHeaders(new HashMap<>());
        }
        int offset = store.pos();
        message.getHeaders().put("X-offset", String.valueOf(offset));
        store.write(message);
        return offset;
    }

    public LclMessage<?> recv(int offset){
        return store.read(offset);
    }

    public void subscribe(MessageSubscription subscription){
        subscriptions.putIfAbsent(subscription.getConsumerId(), subscription);
    }

    public static void sub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        log.info("=====>>> sub {}", subscription);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription){
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        log.info("=====>>> unsub {}", subscription);
        if(messageQueue == null){
            return;
        }
        messageQueue.unSubscribe(subscription);
    }

    private void unSubscribe(MessageSubscription subscription) {
        subscriptions.remove(subscription.getConsumerId());
    }

    public static int send(String topic, LclMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        log.info("=====>>> send topic/message for {}/{}", topic, message);
        return messageQueue.send(message);
    }


    /**
     * 使用此方法，需要手动调用 ack 手动更新 offset
     * @param topic
     * @param consumerId
     * @return
     */
    public static LclMessage<?> recv(String topic, String consumerId, int offset) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        if(!messageQueue.subscriptions.containsKey(consumerId)){
            throw new RuntimeException("subscription not found for topic/consumerId：" + topic + "/" + consumerId);
        }
        log.info("=====>>> recv:topic/cid/offset for {}/{}/{}", topic, consumerId, offset);
        LclMessage<?> recv = messageQueue.recv(offset);
        log.info("=====>>> recv message：{}", recv);
        return recv;
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
        // 如果没有消息被确认（offset=-1），则从第一条开始消费（nextMsgOffset = 0）
        // 如果有消息被确认，则从被确认的下一条消息开始消费（nextMsgOffset = offset + entry.getLength()）
        int nextMsgOffset = 0;
        int offset = messageQueue.subscriptions.get(consumerId).getOffset();
        if(offset > -1){
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            nextMsgOffset = offset + entry.getLength();
        }
        log.info("=====>>> recv:topic/cid/offset for {}/{}/{}", topic, consumerId, nextMsgOffset);
        LclMessage<?> recv = messageQueue.recv(nextMsgOffset);
        log.info("=====>>> recv message：{}", recv);
        return recv;
    }


    public static List<LclMessage<?>> batchRecv(String topic, String consumerId, int size) {
        MessageQueue messageQueue = queues.get(topic);
        if(messageQueue == null){
            throw new RuntimeException("topic not found");
        }
        if(!messageQueue.subscriptions.containsKey(consumerId)){
            throw new RuntimeException("subscription not found for topic/consumerId：" + topic + "/" + consumerId);
        }
        int offset = messageQueue.subscriptions.get(consumerId).getOffset();
        int nextMsgOffset = 0;
        if(offset > -1){
            Indexer.Entry entry = Indexer.getEntry(topic, offset);
            nextMsgOffset = offset + entry.getLength();
        }
        List<LclMessage<?>> result = new ArrayList<>();
        LclMessage<?> recv = messageQueue.recv(nextMsgOffset);
        while (recv != null) {
            result.add(recv);
            if(result.size() >= size){
                break;
            }
            nextMsgOffset += Indexer.getEntry(topic, nextMsgOffset).getLength();
            recv = messageQueue.recv(nextMsgOffset);
        }
        log.info("=====>>> recvs:topic/cid/offset/size for {}/{}/{}/{}", topic, consumerId, nextMsgOffset, result.size());
        log.info("=====>>> last smessage：{}", recv);
        return result;
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
        if(offset <= messageSubscription.getOffset() || offset >= Store.LEN){
            log.error("offset illegality：" + offset);
            return -1;
        }
        log.info("=====>>> ack:topic/cid/offset for {}/{}/{}", topic, consumerId, offset);
        messageSubscription.setOffset(offset);
        return offset;
    }
}
