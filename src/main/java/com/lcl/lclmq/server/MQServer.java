package com.lcl.lclmq.server;

import com.lcl.lclmq.model.LclMessage;
import com.lcl.lclmq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * MQ Server
 * @Author conglongli
 * @date 2024/7/13 22:10
 */
@Controller
@RequestMapping("/lclmq")
public class MQServer {

    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t")String topic, @RequestParam("cid")String consumerId, @RequestBody LclMessage<String> message){
        int sendId = MessageQueue.send(topic, consumerId, message);
        return Result.ok("send ok " + sendId);
    }

    // recv
    @RequestMapping("/recv")
    public Result<LclMessage<?>> recv(@RequestParam("t")String topic, @RequestParam("cid")String consumerId){
        LclMessage<?> message = MessageQueue.recv(topic, consumerId);
        return Result.msg(message);
    }

    // ack
    @RequestMapping("/ack")
    public Result<LclMessage<String>> ack(@RequestParam("t")String topic, @RequestParam("cid")String consumerId, @RequestParam("offset")Integer offset){
        int ackOffset = MessageQueue.ack(topic, consumerId, offset);
        return Result.ok(String.valueOf(ackOffset));
    }


    // sub
    @RequestMapping("/sub")
    public Result<String> subscribe(@RequestParam("t")String topic, @RequestParam("cid")String consumerId){
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsub
    @RequestMapping("/unsub")
    public Result<String> unSubscribe(@RequestParam("t")String topic, @RequestParam("cid")String consumerId){
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
