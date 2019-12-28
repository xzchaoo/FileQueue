package com.xzchaoo.filequeue.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * @author xzchaoo
 */
class FileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
    }

    static void unmap(MappedByteBuffer mbb) {
        if (mbb instanceof DirectBuffer) {
            Cleaner c = ((DirectBuffer) mbb).cleaner();
            if (c != null) {
                c.clean();
            }
        }
    }

    static MappedByteBuffer map(File file, boolean readonly, long length) throws IOException {
        // RandomAccessFile 和 FileChannel 都可以关闭 并不会影响 MappedByteBuffer
        try ( RandomAccessFile raf = new RandomAccessFile(file, readonly ? "r" : "rw") ) {
            if (!readonly) {
                raf.setLength(length);
            }
            try ( FileChannel channel = raf.getChannel() ) {
                return channel.map(readonly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                    0, length);
            }
        }
    }

    static void tryDelete(File file) {
        if (!file.delete()) {
            LOGGER.warn("Fail to delete file {}", file);
        }
    }
}
