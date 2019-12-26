package com.xzchaoo.filequeue.core;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xzchaoo
 */
final class FileUtils {
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
        // 对于已经存在的文件 length 必须和源文件一样或更大
        if (file.exists()) {
            length = Math.max(file.length(), length);
        }
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
            file.deleteOnExit();
            LOGGER.warn("fail to delete file {}", file);
        }
    }

    /**
     * 删除所有 < readerFileIndex 的文件
     */
    static void deleteUnusedFiles(File dir, int readerFileIndex) {
        File[] files = dir.listFiles(new DataFileFilter());
        if (files == null) {
            return;
        }
        for (File file : files) {
            String name = file.getName();
            String[] ss = name.split("\\.", 2);
            String basename = ss[0];
            int index = Integer.parseInt(basename);
            if (index < readerFileIndex) {
                tryDelete(file);
            }
        }
    }

    static void deleteDirectory(File dir) {
        if (dir == null || !dir.exists() || dir.isFile()) {
            return;
        }

        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile()) {
                if (!file.delete()) {
                    throw new IllegalStateException("Fail to delete file " + file);
                }
            } else {
                deleteDirectory(file);
            }
        }
    }

    static void copyFile(File source, File target) throws IOException {
        Files.copy(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private static class DataFileFilter implements FileFilter {
        @Override
        public boolean accept(File f) {
            return f.isFile() && f.getName().endsWith(".data");
        }
    }
}
