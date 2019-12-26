package com.xzchaoo.filequeue.core;

import java.nio.ByteBuffer;

/**
 * @author xiangfeng.xzc
 * @date 2019-12-22
 */
public abstract class AbstractFileQueue implements FileQueue {
    @Override
    public void enqueue(byte[] data) {
        enqueue(ByteBuffer.wrap(data));
    }

    @Override
    public void enqueue(byte[] data, int offset, int length) {
        enqueue(ByteBuffer.wrap(data, offset, length));
    }

    @Override
    public byte[] dequeue() {
        return ByteBufferUtils.readAllToByteArray(dequeueByteBuffer());
    }

    @Override
    public byte[] peek() {
        return ByteBufferUtils.readAllToByteArray(peekByteBuffer());
    }
}
