package com.lcl.lclmq.core;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.Objects;

/**
 * lcl message model
 * @Author conglongli
 * @date 2024/7/11 20:49
 */
@Data
@AllArgsConstructor
public class LclMessage<T> {
//    private String topic;
    private Long id;
    private T body;
    // 系统属性
    private Map<String, String> headers;
    // 业务属性
    private Map<String, String> properties;

}
