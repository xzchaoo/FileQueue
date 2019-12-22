package com.xzchaoo.filequeue.core;

/**
 * @author xiangfeng.xzc
 * @date 2019-09-28
 */
public interface MpscFileQueue extends FileQueue {
    default void enqueue(byte[] data) {
        enqueue(data, false);
    }

    void enqueue(byte[] data, boolean wait);

    default void flush() {
        flush(false);
    }

    void flush(boolean wait);
}
