package com.xzchaoo.filequeue.core;

import java.nio.ByteBuffer;

/**
 * 是否线程安全需要看具体的子类
 *
 * @author xzchaoo
 */
public interface FileQueue {
    /**
     * 入队
     *
     * @param data
     */
    void enqueue(byte[] data);

    /**
     * 入队
     *
     * @param data
     */
    void enqueue(byte[] data, int offset, int length);

    /**
     * 入队
     *
     * @param data
     */
    void enqueue(ByteBuffer data);

    /**
     * 出队
     *
     * @return 出队的数据, 如果没有则返回null
     */
    byte[] dequeue();

    /**
     * 出队, 返回ByteBuffer
     *
     * @return
     */
    ByteBuffer dequeueByteBuffer();

    /**
     * 跳过下一个元素
     *
     * @return
     */
    boolean skip();

    /**
     * peek下一个元素
     *
     * @return 下一个byte[], 如果不存在则返回null
     */
    byte[] peek();

    /**
     * peek下一个元素
     *
     * @return 下一个ByteBuffer, 如果不存在则返回null
     */
    ByteBuffer peekByteBuffer();

    /**
     * 队列里是否有数据
     *
     * @return
     */
    boolean hasData();

    /**
     * 将内存里的缓存flush到底层存储
     */
    void flush();

    /**
     * 是否底层资源, 不可再用
     */
    void close();
}
