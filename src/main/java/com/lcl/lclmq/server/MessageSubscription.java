package com.lcl.lclmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author conglongli
 * @date 2024/7/13 22:21
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;

}
