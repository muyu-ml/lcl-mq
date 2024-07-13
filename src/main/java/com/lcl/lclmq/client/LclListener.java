package com.lcl.lclmq.client;

import com.lcl.lclmq.model.LclMessage;

/**
 * @Author conglongli
 * @date 2024/7/11 21:32
 */
public interface LclListener<T> {
    void onMessage(LclMessage<T> message);
}
