package com.xzchaoo.filequeue.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO 加入CRC功能 可以使用Disruptor来加速! 基于文件系统的Queue. 非线程安全的, 如果并发访问, 需要自己在外部进行同步. 另外Queue的使用场景都是"顺序", 理论上并发并不会快多少.
 *
 * @author xzchaoo
 */
@SuppressWarnings("WeakerAccess")
// @NotThreadSafe
public class SpscFileQueue extends AbstractFileQueue {
    private static final Logger            LOGGER                     = LoggerFactory.getLogger(SpscFileQueue.class);
    /**
     * 默认数据文件大小
     */
    public static final  long              DEFAULT_FILE_SIZE          = 64 * 1024 * 1024L;
    /**
     * 空记录的标记
     */
    private static final byte              MARK_EMPTY                 = 0;
    /**
     * 数据记录的标记
     */
    private static final byte              MARK_DATA                  = 1;
    /**
     * 结束标记
     */
    private static final byte              MARK_END                   = 2;
    private static final byte[]            SKIP_MARK_DATA             = new byte[0];
    private static final ByteBuffer        SKIP_MARK_DATA_BYTE_BUFFER = ByteBuffer.allocate(0);
    private              SpscFileQueueSpec spec;
    /**
     * 数据文件大小
     */
    private              long              fileSize;
    /**
     * 最大允许的文件数
     */
    private              int               maxAllowedDataFileCount;
    /**
     * 单条数据最大大小
     */
    private              int               maxDataSize;
    /**
     * 元信息
     */
    private              FileQueueMeta     meta;
    /**
     * 写buffer
     */
    private volatile     MappedByteBuffer  writerBuffer;
    /**
     * 读buffer
     */
    private volatile     MappedByteBuffer  readerBuffer;

    /**
     * @param dir
     * @throws IOException
     * @deprecated use {@link FileQueues }
     */
    @Deprecated
    public SpscFileQueue(File dir) throws IOException {
        this(dir, DEFAULT_FILE_SIZE, -1);
    }

    SpscFileQueue(File dir, long fileSize, int maxAllowedDataFileCount) throws IOException {
        SpscFileQueueSpec spec = new SpscFileQueueSpec();
        spec.setDir(dir);
        spec.setFileSize(fileSize);
        spec.setMaxAllowedDataFileCount(maxAllowedDataFileCount);
        init(spec);
    }

    SpscFileQueue(SpscFileQueueSpec spec) throws IOException {
        init(spec);
    }

    private void init(SpscFileQueueSpec spec) throws IOException {
        this.spec = Objects.requireNonNull(spec);
        File dir = Objects.requireNonNull(spec.getDir());

        if (spec.meta == null) {
            spec.meta = new FileQueueMeta(dir);
        }
        this.meta = spec.meta;

        this.fileSize = spec.getFileSize() <= 0 ? DEFAULT_FILE_SIZE : spec.getFileSize();
        if (Long.bitCount(fileSize) != 1) {
            throw new IllegalStateException("FileSize must be power of 2");
        }

        this.maxDataSize = spec.getMaxDataSize() <= 0 ? (int) (fileSize / 2) : spec.getMaxDataSize();
        this.maxAllowedDataFileCount = spec.getMaxAllowedDataFileCount();

        // 初始化删除无用文件
        FileUtils.deleteUnusedFiles(dir, meta.readerFileIndex);

        writerBuffer = this.meta.openWriterBuffer(fileSize);
        ByteBufferUtils.fillRemainWithZero(writerBuffer);

        readerBuffer = this.meta.openReaderBuffer(fileSize);
    }

    public void log() {
        LOGGER.info("fileIndex={}+{}={} index={}+{}={}",
                meta.getReaderFileIndex(),
                meta.getWriterFileIndex() - meta.getReaderFileIndex(),
                meta.getWriterFileIndex(),
                meta.getReaderIndex(),
                meta.getWriterIndex() - meta.getReaderIndex(),
                meta.getWriterIndex()
        );
    }

    private MappedByteBuffer map(File file, boolean readonly) {
        try {
            return FileUtils.map(file, readonly, fileSize);
        } catch (IOException e) {
            throw new IllegalStateException("Fail to map file " + file, e);
        }
    }

    @Override
    public void enqueue(ByteBuffer data) {
        int length = data.remaining();
        if (length >= maxDataSize) {
            throw new IllegalArgumentException();
        }
        int remain = writerBuffer.remaining();
        // 刚好等于也要换文件 因为每个文件一定会以END结尾
        if (remain <= 5 + length) {
            switchToNextWriterFile();
        }
        writerBuffer.put(MARK_DATA);
        writerBuffer.putInt(length);
        writerBuffer.put(data);
        meta.incrementWriter(writerBuffer.position());
    }

    private void switchToNextWriterFile() {
        // 数量限制
        if (maxAllowedDataFileCount > 0) {
            int distance = meta.getWriterFileIndex() - meta.getReaderFileIndex();
            if (distance > maxAllowedDataFileCount) {
                throw new IllegalStateException("there are to many data files");
            }
        }
        // 提前结束
        if (writerBuffer.position() < writerBuffer.capacity()) {
            writerBuffer.put(MARK_END);
        }
        writerBuffer.force();
        FileUtils.unmap(writerBuffer);
        meta.switchToNextWriteFile();
        writerBuffer = meta.openWriterBuffer(fileSize);
    }

    @Override
    public ByteBuffer dequeueByteBuffer() {
        return doDequeueByteBuffer(false, false);
    }

    private ByteBuffer doDequeueByteBuffer(boolean skip, boolean peek) {
        if (!hasData()) {
            return null;
        }
        // TODO 分析一下这句话有可能执行到???
        if (readerBuffer.position() == readerBuffer.capacity()) {
            switchToNextReadFile();
        }

        while (true) {
            // peek
            byte flag = readerBuffer.get(readerBuffer.position());
            switch (flag) {
                case MARK_EMPTY:
                    throw new IllegalStateException("期望有数据却遇到EMPTY");
                case MARK_DATA:
                    int oldPosition = readerBuffer.position();
                    ByteBuffer data;
                    ByteBufferUtils.skip(readerBuffer, 1);
                    int dataLength = readerBuffer.getInt();
                    int remaining = readerBuffer.remaining();
                    // 如果数据损坏可能会出这样的问题
                    if (dataLength < 0 || dataLength > remaining) {
                        throw new IllegalStateException("Data is broken");
                    }
                    if (skip) {
                        data = SKIP_MARK_DATA_BYTE_BUFFER;
                    } else {
                        data = readerBuffer.slice();
                        data.limit(dataLength);
                    }
                    ByteBufferUtils.skip(readerBuffer, dataLength);
                    if (peek) {
                        readerBuffer.position(oldPosition);
                    } else {
                        meta.incrementReader(readerBuffer.position());
                    }
                    return data;
                case MARK_END:
                    switchToNextReadFile();
                    continue;
                default:
                    throw new IllegalStateException("Unknown flag " + flag);
            }
        }
    }

    @Override
    public boolean skip() {
        return doDequeueByteBuffer(true, false) == SKIP_MARK_DATA_BYTE_BUFFER;
    }

    @Override
    public ByteBuffer peekByteBuffer() {
        return doDequeueByteBuffer(false, true);
    }

    private void switchToNextReadFile() {
        FileUtils.unmap(readerBuffer);
        FileUtils.tryDelete(meta.getReaderFile());
        meta.switchToNextReadFile();
        readerBuffer = this.meta.openReaderBuffer(fileSize);
    }

    @Override
    public boolean hasData() {
        // 可见性
        return meta.readerIndex < meta.writerIndex;
    }

    @Override
    public void flush() {
        writerBuffer.force();
        meta.saveWriter(writerBuffer.position());
        meta.saveReader(readerBuffer.position());
    }

    @Override
    public void close() {
        flush();
    }

    public FileQueueMeta getMeta() {
        return meta;
    }
}
