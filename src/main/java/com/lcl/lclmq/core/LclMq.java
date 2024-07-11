package com.lcl.lclmq.core;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * mq for topic
 * @Author conglongli
 * @date 2024/7/11 21:01
 */
@AllArgsConstructor
public class LclMq {

    public LclMq(String topic){
        this.topic = topic;
    }

    private String topic;

    private LinkedBlockingQueue<LclMessage> queue = new LinkedBlockingQueue();

    private List<LclListener> listeners = new ArrayList<>();

    public boolean send(LclMessage message) {
        boolean offered = queue.offer(message);
        listeners.forEach(listener -> listener.onMessage(message));
        return offered;
    }

    /**
     * 拉模式获取消息
     * @param timeout
     * @return
     * @param <T>
     */
    @SneakyThrows
    public <T> LclMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListen(LclListener<T> listener) {
        listeners.add(listener);
    }
}
