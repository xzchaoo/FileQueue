package com.xzchaoo.filequeue.core;

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
     * 出队
     *
     * @return 出队的数据, 如果没有则返回null
     */
    byte[] dequeue();

    boolean skip();

    /**
     * 检查
     *
     * @return
     */
    byte[] peek();

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
