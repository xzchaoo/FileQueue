package com.xzchaoo.filequeue.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Objects;

/**
 * 可以使用Disruptor来加速! 基于文件系统的Queue. 非线程安全的, 如果并发访问, 需要自己在外部进行同步. 另外Queue的使用场景都是"顺序", 理论上并发并不会快多少.
 *
 * @author xzchaoo
 */
@SuppressWarnings("WeakerAccess")
// @NotThreadSafe
public class SingleThreadFileQueue implements FileQueue {
    public static final long DEFAULT_FILE_SIZE = 64 * 1024 * 1024L;

    private static final Logger LOGGER       = LoggerFactory.getLogger(SingleThreadFileQueue.class);
    private static final int    OS_PAGE_SIZE = 4096;
    private static final byte   EMPTY        = 0;
    private static final byte   DATA         = 1;
    private static final byte   END          = 2;

    private final long fileSize;
    private final int  maxAllowedDataFileCount;

    /**
     * 数据目录
     */
    private final File dir;

    /**
     * 元信息
     */
    private final Meta meta;

    /**
     * 写文件的索引
     */
    private volatile int writerFileIndex;

    private volatile long writerIndex;

    /**
     * 写buffer
     */
    private volatile MappedByteBuffer writerBuffer;

    /**
     * 读文件的索引
     */
    private volatile int readerFileIndex;

    private volatile long readerIndex;

    /**
     * 读buffer
     */
    private volatile MappedByteBuffer readerBuffer;

    public SingleThreadFileQueue(File dir) throws IOException {
        this(dir, DEFAULT_FILE_SIZE, -1);
    }

    public SingleThreadFileQueue(File dir, long fileSize, int maxAllowedDataFileCount) throws IOException {
        this.dir = Objects.requireNonNull(dir);
        // TODO 2^n 保证
        this.fileSize = fileSize;

        this.maxAllowedDataFileCount = maxAllowedDataFileCount;

        // ensure directory exists
        if (!dir.exists() && !dir.mkdirs()) {
            LOGGER.warn("fail to mkdirs {}", dir);
        }

        this.meta = new Meta();

        meta.log();

        // 初始化删除无用文件
        deleteUnusedFiles();

        // 启动定时任务删除数据
        // TODO 我们在readerFileIndex前进的时候删除旧文件 不需要太多资源
    }

    public void log() {
        meta.log();
    }

    private MappedByteBuffer map(int fileIndex, boolean readonly) {
        File file = dataFile(fileIndex);
        try {
            return FileUtils.map(file, readonly, fileSize);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void enqueue(byte[] data) {
        if (data.length >= fileSize / 2) {
            throw new IllegalArgumentException();
        }

        int remain = writerBuffer.capacity() - writerBuffer.position();
        if (remain < 5 + data.length) {
            meta.switchToNextWriteFile();
        }
        writerBuffer.put(DATA);
        writerBuffer.putInt(data.length);
        writerBuffer.put(data);
        ++writerIndex;
        meta.saveWriter();
    }

    @Override
    public byte[] dequeue() {
        if (!hasData()) {
            return null;
        }

        if (readerBuffer.position() == readerBuffer.capacity()) {
            meta.switchToNextReadFile();
            return dequeue();
        }

        // peed
        byte flag = readerBuffer.get(readerBuffer.position());
        // TODO 空文件是全填充成0吗?
        switch (flag) {
            case EMPTY:
                return null;
            case DATA:
                // consume
                readerBuffer.get();
                int dataLength = readerBuffer.getInt();
                byte[] data = new byte[dataLength];
                readerBuffer.get(data);
                ++readerIndex;
                meta.saveReader();
                return data;
            case END:
                meta.switchToNextReadFile();
                return dequeue();
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public boolean skip() {
        if (!hasData()) {
            return false;
        }

        if (readerBuffer.position() == readerBuffer.capacity()) {
            meta.switchToNextReadFile();
            return skip();
        }

        // peed
        byte flag = readerBuffer.get(readerBuffer.position());
        // TODO 空文件是全填充成0吗?
        switch (flag) {
            case EMPTY:
                return false;
            case DATA:
                // consume
                readerBuffer.get();
                int dataLength = readerBuffer.getInt();
                readerBuffer.position(readerBuffer.position() + dataLength);
                ++readerIndex;
                meta.saveReader();
                return true;
            case END:
                meta.switchToNextReadFile();
                return skip();
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public byte[] peek() {
        if (!hasData()) {
            return null;
        }
        if (readerBuffer.position() == readerBuffer.capacity()) {
            meta.switchToNextReadFile();
            return peek();
        }

        // peek
        byte flag = readerBuffer.get(readerBuffer.position());
        // TODO 空文件是全填充成0吗?
        switch (flag) {
            case EMPTY:
                return null;
            case DATA:
                // peek
                int pos = readerBuffer.position();
                try {
                    readerBuffer.get();
                    int dataLength = readerBuffer.getInt();
                    byte[] data = new byte[dataLength];
                    readerBuffer.get(data);
                    return data;
                } finally {
                    readerBuffer.position(pos);
                }
            case END:
                meta.switchToNextReadFile();
                return peek();
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public boolean hasData() {
        // 可见性
        return readerIndex < writerIndex;
    }

    @Override
    public void flush() {
        meta.save(true);
    }

    @Override
    public void close() {
        // do nothing
        flush();
    }

    /**
     * 删除所有 < readerFileIndex 的文件
     */
    private void deleteUnusedFiles() {
        File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".data"));
        if (files == null) {
            return;
        }
        for (File file : files) {
            String name = file.getName();
            String[] ss = name.split("\\.", 2);
            String basename = ss[0];
            int index = Integer.parseInt(basename);
            if (index < readerFileIndex) {
                FileUtils.tryDelete(file);
            }
        }
    }

    private File dataFile(int index) {
        return new File(dir, index + ".data");
    }

    private class Meta {
        final File             writerMetaFile;
        final MappedByteBuffer writerMetaBuffer;

        final File             readerMetaFile;
        final MappedByteBuffer readerMetaBuffer;

        Meta() throws IOException {
            writerMetaFile = new File(dir, "writer.meta");
            readerMetaFile = new File(dir, "reader.meta");

            writerMetaBuffer = FileUtils.map(writerMetaFile, false, OS_PAGE_SIZE);
            readerMetaBuffer = FileUtils.map(readerMetaFile, false, OS_PAGE_SIZE);

            load();
        }

        void load() {
            // TODO 文件不存在咋整? 我们的值刚好都是0 所以刚好不要紧

            writerFileIndex = writerMetaBuffer.getInt(0);
            writerBuffer = map(writerFileIndex, false);
            writerBuffer.position(writerMetaBuffer.getInt(4));
            writerIndex = writerMetaBuffer.getLong(8);

            readerFileIndex = readerMetaBuffer.getInt(0);
            readerBuffer = map(readerFileIndex, true);
            readerBuffer.position(readerMetaBuffer.getInt(4));
            readerIndex = readerMetaBuffer.getLong(8);
        }

        void saveWriter() {
            writerMetaBuffer.putInt(0, writerFileIndex);
            writerMetaBuffer.putInt(4, writerBuffer.position());
            writerMetaBuffer.putLong(8, writerIndex);
        }

        void saveReader() {
            readerMetaBuffer.putInt(0, readerFileIndex);
            readerMetaBuffer.putInt(4, readerBuffer.position());
            readerMetaBuffer.putLong(8, readerIndex);
        }

        void save() {
            save(false);
        }

        /**
         * 注意该方法的可见性/并发问题
         */
        void save(boolean force) {
            saveReader();
            saveWriter();

            if (force) {
                writerBuffer.force();
                readerBuffer.force();
                writerMetaBuffer.force();
                readerMetaBuffer.force();
            }
        }

        void switchToNextReadFile() {
            FileUtils.unmap(readerBuffer);
            // TODO buffer 没回收之前能删掉文件吗?
            FileUtils.tryDelete(dataFile(readerFileIndex));
            readerBuffer = map(++readerFileIndex, true);
            meta.saveReader();
        }

        void switchToNextWriteFile() {
            // 数量限制
            if (maxAllowedDataFileCount > 0) {
                int distance = writerFileIndex - readerFileIndex;
                if (distance > maxAllowedDataFileCount) {
                    throw new IllegalStateException("there are to many data files");
                }
            }

            // 提前结束
            if (writerBuffer.position() < writerBuffer.capacity()) {
                writerBuffer.put(END);
            }
            writerBuffer.force();
            FileUtils.unmap(writerBuffer);

            writerBuffer = map(++writerFileIndex, false);
            saveWriter();
        }

        public void log() {
            LOGGER.info("writerFileIndex={} readerFileIndex={} writerBuffer.position={} readerBuffer.position={}",
                    writerFileIndex,
                    readerFileIndex,
                    writerBuffer.position(),
                    readerBuffer.position()
            );
        }
    }

    public int getWriterFileIndex() {
        return writerFileIndex;
    }

    public long getWriterIndex() {
        return writerIndex;
    }

    public int getReaderFileIndex() {
        return readerFileIndex;
    }

    public long getReaderIndex() {
        return readerIndex;
    }
}
