package com.lcl.lclmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author conglongli
 * @date 2024/7/11 21:18
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private long id;
    private String item;
    private double price;
}
