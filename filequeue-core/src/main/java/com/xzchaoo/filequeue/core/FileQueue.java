package com.xzchaoo.filequeue.core;

import java.nio.ByteBuffer;

/**
 * File based FIFO queue.
 *
 * @author xzchaoo
 */
public interface FileQueue {
    /**
     * Enqueue data.
     * Data is not guaranteed to be flushed to disk when this method returns.
     *
     * @param data
     */
    void enqueue(byte[] data);

    /**
     * Enqueue data.
     * Data is not guaranteed to be flushed to disk when this method returns.
     *
     * @param data   byte array data
     * @param offset data offset
     * @param length data length
     */
    void enqueue(byte[] data, int offset, int length);

    /**
     * Enqueue data.
     * Data is not guaranteed to be flushed to disk when this method returns.
     *
     * @param data byte buffer data
     */
    void enqueue(ByteBuffer data);

    /**
     * Dequeue data.
     *
     * @return dequeued data
     */
    byte[] dequeue();

    /**
     * Skip next data.
     *
     * @return true if there has any data in queue else null.
     */
    boolean skip();

    /**
     * Peek next data.
     *
     * @return byte array if there has any data in queue else null.
     */
    byte[] peek();

    /**
     * Check if queue has data.
     *
     * @return true if there has any data in queue else false.
     */
    boolean hasData();

    /**
     * Flush write buffer to disk. This method should be call on writer thread.
     */
    void flush();

    /**
     * Close IO resources. Now, this method acts same as {@link #flush() } method.
     */
    void close();
}
