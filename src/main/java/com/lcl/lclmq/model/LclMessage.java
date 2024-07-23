package com.lcl.lclmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * lcl message model
 * @Author conglongli
 * @date 2024/7/11 20:49
 */
@Data
@AllArgsConstructor
public class LclMessage<T> {
//    private String topic;
    static AtomicLong idgen = new AtomicLong(0);
    private Long id;
    private T body;
    // 系统属性
    private Map<String, String> headers = new HashMap<>();
    // 业务属性
    //private Map<String, String> properties;

    public static long nextId(){
        return idgen.getAndIncrement();
    }

    public static LclMessage<String> create(String body, Map<String, String> headers) {
        return new LclMessage(nextId(), body, headers);
    }

}
