package com.lcl.lclmq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lcl.lclmq.model.LclMessage;
import com.lcl.lclmq.model.Result;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics
 * @Author conglongli
 * @date 2024/7/11 20:59
 */
@Slf4j
public class LclBroker {

    @Getter
    public static LclBroker Default = new LclBroker();
    public static String brokerUrl = "http://localhost:8765/lclmq";

    static {
        init();
    }

    private static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            MultiValueMap<String, LclConsumer> consumers = getDefault().getConsumers();
            consumers.forEach((topic, consumers1) -> {
                consumers1.forEach(consumer -> {
                    LclMessage<?> recv = consumer.recv(topic);
                    if(recv == null){
                        return;
                    }
                    try {
                        consumer.getListener().onMessage(recv);
                    } catch (Exception e) {
                        // todo
                    }
                    consumer.ack(topic, recv);
                });
            });
        }, 100, 100);
    }


    public LclProducer createProducer(){
        return new LclProducer(this);
    }

    public LclConsumer<?> createConsumer(String topic) {
        LclConsumer<?> consumer = new LclConsumer<>(this);
        consumer.sub(topic);
        return consumer;
    }

    public boolean send(String topic, LclMessage message) {
        log.info("s===>>>> send topic/message {}/{}", topic, message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message), brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>(){});
        log.info("===>>>> send result {}", JSON.toJSONString(result));
        return result.getCode() == 1;
    }

    public void sub(String topic, String id) {
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + id, new TypeReference<Result<String>>(){});
        log.info("===>>>> sub result {}", JSON.toJSONString(result));
    }

    public <T> LclMessage<T> recv(String topic, String id) {
        Result<LclMessage<String>> result = HttpUtils.httpGet(brokerUrl + "/recv?t=" + topic + "&cid=" + id, new TypeReference<Result<LclMessage<String>>>(){});
        log.info("===>>>> recv topic/message {}/{}", topic, id);
        return (LclMessage<T>) result.getData();
    }

    public void unsub(String topic, String id) {
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + id, new TypeReference<Result<String>>(){});
        log.info("===>>>> unsub result {}", JSON.toJSONString(result));
    }

    public boolean ack(String topic, String cid, int offset) {
        log.info("===>>>> ack topic/cid/offset：{}/{}/{}", topic, cid, offset);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic + "&cid=" + cid + "&offset=" + offset, new TypeReference<Result<String>>(){});
        log.info("===>>>> ack result {}", JSON.toJSONString(result));
        return result.getCode() == 1;
    }

    @Getter
    private MultiValueMap<String, LclConsumer> consumers = new LinkedMultiValueMap();
    public <T> void addListen(String topic, LclConsumer<?> consumer) {
        consumers.add(topic, consumer);
    }
}
