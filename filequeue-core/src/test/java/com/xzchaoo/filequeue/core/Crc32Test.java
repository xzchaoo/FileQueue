package com.xzchaoo.filequeue.core;

import org.junit.Test;

import java.util.zip.CRC32;

/**
 * @author xiangfeng.xzc
 * @date 2019-09-28
 */
public class Crc32Test {
    @Test
    public void test() {
        // TODO 与 murmur32 相比性能怎么样
        CRC32 c = new CRC32();
        c.update(1);
        c.update(2);
        System.out.println((int) c.getValue());
    }
}
