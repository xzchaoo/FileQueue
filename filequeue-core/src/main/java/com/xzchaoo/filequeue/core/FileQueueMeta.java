package com.xzchaoo.filequeue.core;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Objects;

/**
 * @author xiangfeng.xzc
 * @date 2019-12-22
 */
public class FileQueueMeta {
    private static final String           DATA_POSTFIX = ".data";
    /**
     * 内存分页大小
     */
    private static final int              OS_PAGE_SIZE = 4096;
    private final        File             dir;
    private final        File             writerMetaFile;
    private              MappedByteBuffer writerMetaBuffer;
    /**
     * 写索引
     */
    volatile             int              writerFileIndex;
    /**
     * 写文件下一个写位置的offset
     */
    volatile             long             writerIndex;
    private final        File             readerMetaFile;
    private              MappedByteBuffer readerMetaBuffer;
    /**
     * 读文件的索引
     */
    volatile             int              readerFileIndex;
    /**
     * 读索引
     */
    volatile             long             readerIndex;

    public FileQueueMeta(File dir) throws IOException {
        this(dir, "");
    }

    FileQueueMeta(File dir, String prefix) throws IOException {
        this.dir = Objects.requireNonNull(dir);
        writerMetaFile = new File(dir, prefix + "writer.meta");
        readerMetaFile = new File(dir, prefix + "reader.meta");

        writerMetaBuffer = FileUtils.map(writerMetaFile, false, OS_PAGE_SIZE);
        readerMetaBuffer = FileUtils.map(readerMetaFile, false, OS_PAGE_SIZE);

        // 如果文件不存在 那么默认是填充0 刚好是我们要的值
        writerFileIndex = writerMetaBuffer.getInt(0);
        writerIndex = writerMetaBuffer.getLong(8);

        readerFileIndex = readerMetaBuffer.getInt(0);
        readerIndex = readerMetaBuffer.getLong(8);

        if (writerFileIndex == readerFileIndex) {
            boolean updated = false;
            if (readerIndex > writerIndex) {
                readerIndex = writerIndex;
                updated = true;
            }
            int readerFileOffset = getReaderFileOffset();
            int writerFileOffset = getWriterFileOffset();
            if (updated || readerFileOffset > writerFileOffset) {
                saveReader(writerFileOffset);
            }
        }
    }

    void saveWriter(int position) {
        writerMetaBuffer.putInt(0, writerFileIndex);
        writerMetaBuffer.putInt(4, position);
        writerMetaBuffer.putLong(8, writerIndex);
        writerMetaBuffer.force();
    }

    public void saveReader(int position) {
        readerMetaBuffer.putInt(0, readerFileIndex);
        readerMetaBuffer.putInt(4, position);
        readerMetaBuffer.putLong(8, readerIndex);
        readerMetaBuffer.force();
    }

    void switchToNextReadFile() {
        ++readerFileIndex;
        this.saveReader(0);
    }

    void switchToNextWriteFile() {
        ++writerFileIndex;
        this.saveWriter(0);
    }

    public File getWriterMetaFile() {
        return writerMetaFile;
    }

    public File getReaderMetaFile() {
        return readerMetaFile;
    }

    public int getWriterFileIndex() {
        return this.writerFileIndex;
    }

    public long getWriterIndex() {
        return this.writerIndex;
    }

    public int getReaderFileIndex() {
        return this.readerFileIndex;
    }

    public long getReaderIndex() {
        return this.readerIndex;
    }

    public int getWriterFileOffset() {
        return this.writerMetaBuffer.getInt(4);
    }

    public int getReaderFileOffset() {
        return this.readerMetaBuffer.getInt(4);
    }

    public void incrementReader(int newPosition) {
        readerMetaBuffer.putInt(4, newPosition);
        readerMetaBuffer.putLong(8, ++readerIndex);
    }

    public void incrementWriter(int newPosition) {
        writerMetaBuffer.putInt(4, newPosition);
        writerMetaBuffer.putLong(8, ++writerIndex);
    }

    public FileQueueMeta generateMetaForValidation() throws IOException {
        FileUtils.copyFile(this.writerMetaFile, new File(dir, "validation_writer.meta"));
        FileUtils.copyFile(this.readerMetaFile, new File(dir, "validation_reader.meta"));
        return new FileQueueMeta(dir, "validation_");
    }

    public File getReaderFile() {
        return new File(dir, readerFileIndex + DATA_POSTFIX);
    }

    public File getWriterFile() {
        return new File(dir, writerFileIndex + DATA_POSTFIX);
    }

    public MappedByteBuffer openWriterBuffer(long length) {
        File file = getWriterFile();
        MappedByteBuffer mbb = map(file, false, length);
        mbb.position(getWriterFileOffset());
        ByteBufferUtils.fillRemainWithZero(mbb);
        return mbb;
    }

    public MappedByteBuffer openReaderBuffer(long length) {
        File file = getReaderFile();
        if (!file.exists()) {
            throw new IllegalStateException("Reader file " + file + " does not exist.");
        }
        MappedByteBuffer mbb = map(file, true, length);
        mbb.position(getReaderFileOffset());
        return mbb;
    }

    private MappedByteBuffer map(File file, boolean readonly, long length) {
        MappedByteBuffer mbb;
        try {
            mbb = FileUtils.map(file, readonly, length);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return mbb;
    }

    public void close() {
        FileUtils.unmap(readerMetaBuffer);
        FileUtils.unmap(writerMetaBuffer);
        readerMetaBuffer = null;
        writerMetaBuffer = null;
    }
}
