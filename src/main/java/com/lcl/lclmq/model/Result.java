package com.lcl.lclmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author conglongli
 * @date 2024/7/13 22:29
 */
@Data
@AllArgsConstructor
public class Result<T> {
    // 1: success 0:fail
    private int code;
    private T data;

    public static Result ok() {
        return new Result<>(1, "OK");
    }

    public static Result ok(String msg) {
        return new Result<>(1, msg);
    }

    public static Result<LclMessage<?>> msg(String msg) {
        return new Result<>(1, LclMessage.create(msg, null));
    }

    public static Result<LclMessage<?>> msg(LclMessage message) {
        return new Result<>(1, message);
    }
}
