package com.lcl.lclmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lcl.lclmq.model.LclMessage;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @Author conglongli
 * @date 2024/7/24 00:39
 */
@Slf4j
public class Store {

    private String topic;
    public static final int LEN = 1024 * 1024;

    public Store(String topic) {
        this.topic = topic;
    }

    @Getter
    MappedByteBuffer mappedByteBuffer = null;

    @SneakyThrows
    public void init() {
        File file = new File(topic + ".dat");
        if(!file.exists()){
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        // 对文件创建内容映射缓存区，缓存区提供了读写权限，映射文件的位置是0-1024
        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);
        // 判断文件是否有数据

        // 读前十位，转int，看是否大于0，说明有数据，往后翻len的长度，如果是，继续读，如果是0，则是数据的结尾
        // 确定文件结尾
        // 设定写入开始位置，防止覆盖写入
//        mappedByteBuffer.position(init_pos);
        // 数据量大于10M（可以到80%时就创建文件），创建第二个文件，管理多个数据文件
    }


    public int write(LclMessage<String> lclMessage) {
        log.info("write position -> {}", mappedByteBuffer.position());
        String msg = JSON.toJSONString(lclMessage);

        int length = msg.getBytes(StandardCharsets.UTF_8).length;
//        String format = String.format("%010d", length);
//        msg = format + msg;
//        length += 10;

        int position = mappedByteBuffer.position();
        // 写入索引
        Indexer.addEntry(topic, position, length);
        // 写入数据
        mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));
        return position;
    }

    public int pos() {
        return mappedByteBuffer.position();
    }

    public LclMessage<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        // 根据消息 id 获取 Entry
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        // 设置文件偏移量和长度
        readOnlyBuffer.position(entry.getOffset());
        int len = entry.getLength();
        byte[] bytes = new byte[len];
        // 读取文件并写入bytes
        readOnlyBuffer.get(bytes, 0, len);
        String messageJson = new String(bytes, StandardCharsets.UTF_8);
        log.info("read only ===>>> {}", messageJson);
        LclMessage<String> lclMessage = JSON.parseObject(messageJson, new TypeReference<LclMessage<String>>() {
        });
        return lclMessage;
    }
}
