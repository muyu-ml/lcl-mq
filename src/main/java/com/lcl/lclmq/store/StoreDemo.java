package com.lcl.lclmq.store;

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
import java.util.Scanner;

/**
 * mmap store demo
 * @Author conglongli
 * @date 2024/7/23 15:34
 */
@Slf4j
public class StoreDemo {
    @SneakyThrows
    public static void main(String[] args) {
        // 内存映射：mmap和file sendFile，现在主要用mmap；内存映射用于提升操作性能和简化对文件的处理；目前主流MQ全是这么处理的。
        String context = """
                this is a good file.
                that is a new line for store.
                """;
        // 数据长度
        int length = context.getBytes(StandardCharsets.UTF_8).length;
        log.info("len = {}", length);
        File file = new File("test.dat");
        if(!file.exists()){
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // 对文件创建内容映射缓存区，缓存区提供了读写权限，映射文件的位置是0-1024
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            // 写数据
            for (int i=0; i<10; i++) {
                // 输出写入数据的偏移量
                log.info("{} -> {}", i, mappedByteBuffer.position());
                // 写入数据
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(i + ":" + context));
            }

            length +=2;

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                if(line.equals("q")){
                    break;
                }
                log.info(" in = {}", line);
                int pos = Integer.parseInt(line);
                readOnlyBuffer.position(pos * length);
                byte[] bytes = new byte[length];
                readOnlyBuffer.get(bytes, 0, length);
                log.info("read only ===>>> {}", new String(bytes, StandardCharsets.UTF_8));
            }

        }

    }
}
