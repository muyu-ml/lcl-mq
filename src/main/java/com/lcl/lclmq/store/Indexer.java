package com.lcl.lclmq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * entry indexer
 * @Author conglongli
 * @date 2024/7/24 00:09
 */
public class Indexer {

    /**
     * topic-List<Entry>
     */
    static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();

    /**
     * id-entry
     */
    static Map<Long, Entry> mappings = new HashMap<>();

    @Data
    @AllArgsConstructor
    public static class Entry {
        // 消息id
        long id;
        // 消息偏移量
        int offset;
        // 消息长度
        int length;
    }

    public static void addEntry(String topic, long id, int offset, int length) {
        Entry entry = new Entry(id, offset, length);
        indexes.add(topic, entry);
        mappings.put(id, entry);
    }

    public static List<Entry> getEntries(String topic) {
        return indexes.get(topic);
    }

    public static Entry getEntry(String topic, long id) {
        return mappings.get(id);
    }
}
