package com.xzchaoo.filequeue.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SPSC (Single Producer Single Consumer) style File Queue implementation.
 * This SpscFileQueue can be used by two threads concurrently. One for reade and another for write.
 * One thread reads data from file head, and another thread writes data to file tail.
 * Data is written to files having fixed file size of {@link #fileSize }, but logically acts as a serialized big array.
 * <ul>
 * <li>CRC32 check</li>
 * </ul>
 *
 * @author xzchaoo
 */
@SuppressWarnings("WeakerAccess")
// @NotThreadSafe
public class SpscFileQueue extends AbstractFileQueue {
    private static final Logger            LOGGER            = LoggerFactory.getLogger(SpscFileQueue.class);
    /**
     * Default file size.
     */
    public static final  long              DEFAULT_FILE_SIZE = 64 * 1024 * 1024L;
    /**
     * Mark for EMPTY data.
     */
    private static final byte              MARK_EMPTY        = 0;
    /**
     * Mark for DATA data.
     */
    private static final byte              MARK_DATA         = 1;
    /**
     * Mark for End data.
     */
    private static final byte              MARK_END          = 2;
    /**
     * A special var as a hint of EMPTY data.
     */
    private static final byte[]            SKIP_MARK_DATA    = new byte[0];
    private              SpscFileQueueSpec spec;
    /**
     * File size.
     */
    private              long              fileSize;
    /**
     * Max allowed data file count. When files in disk exceeds this value, it will throws an exception when try to switch to next data
     * file.
     */
    private              int               maxAllowedDataFileCount;
    /**
     * Max data size.
     */
    private              int               maxDataSize;
    /**
     * Meta data.
     */
    private              FileQueueMeta     meta;
    /**
     * Writer buffer.
     */
    private volatile     MappedByteBuffer  writerBuffer;
    /**
     * Reader buffer.
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

    /**
     * Initialize this FileQueue.
     *
     * @param spec
     * @throws IOException
     */
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

    /**
     * Log meta position for debug.
     */
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

    @Override
    public byte[] dequeue() {
        return doDequeue(false, false);
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

    private byte[] doDequeue(boolean skip, boolean peek) {
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
                    byte[] data;
                    ByteBufferUtils.skip(readerBuffer, 1);
                    int dataLength = readerBuffer.getInt();
                    int remaining = readerBuffer.remaining();
                    // 如果数据损坏可能会出这样的问题
                    if (dataLength < 0 || dataLength > remaining) {
                        throw new IllegalStateException("Data is broken");
                    }
                    if (skip) {
                        data = SKIP_MARK_DATA;
                        ByteBufferUtils.skip(readerBuffer, dataLength);
                    } else {
                        data = new byte[dataLength];
                        readerBuffer.get(data);
                    }
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
        return doDequeue(true, false) == SKIP_MARK_DATA;
    }

    @Override
    public byte[] peek() {
        return doDequeue(false, true);
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
