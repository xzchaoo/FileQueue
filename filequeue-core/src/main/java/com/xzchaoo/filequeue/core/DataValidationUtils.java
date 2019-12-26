package com.xzchaoo.filequeue.core;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * @author xiangfeng.xzc
 * @date 2019-12-22
 */
final class DataValidationUtils {
    private DataValidationUtils() {}

    /**
     * <p>检查一个FileQueue是否合法(元数据不损坏, 但data数据可以损坏).
     * <p>元数据损坏会导致读数据无法前进
     * <p>data数据损坏可能导致反序列化失败, 或反序列化成功但字段值错误.
     *
     * @param q
     */
    static void validate(SpscFileQueue q) {

    }

    public int crc32(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(buffer);
        return (int) crc32.getValue();
    }

    public int crc32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }
}
