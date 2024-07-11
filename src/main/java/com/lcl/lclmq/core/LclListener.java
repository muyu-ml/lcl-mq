package com.lcl.lclmq.core;

/**
 * @Author conglongli
 * @date 2024/7/11 21:32
 */
public interface LclListemer<T> {
    void onMessage(LclMessage<T> message);
}
