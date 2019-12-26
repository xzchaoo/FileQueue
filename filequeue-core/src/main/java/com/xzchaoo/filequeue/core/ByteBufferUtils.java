package com.xzchaoo.filequeue.core;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * @author xiangfeng.xzc
 * @date 2019-12-23
 */
final class ByteBufferUtils {
    private static final byte[] ZERO_PAGE = new byte[4096];

    private ByteBufferUtils() {}

    static void skip(Buffer bb, int size) {
        bb.position(bb.position() + size);
    }

    static void fillRemainWithZero(MappedByteBuffer buffer) {
        int position = buffer.position();
        while (buffer.hasRemaining()) {
            int length = Math.min(buffer.remaining(), ZERO_PAGE.length);
            buffer.put(ZERO_PAGE, 0, length);
        }
        buffer.position(position);
    }

    public static byte[] readAllToByteArray(ByteBuffer bb) {
        if (bb == null) {
            return null;
        }
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        return data;
    }
}
