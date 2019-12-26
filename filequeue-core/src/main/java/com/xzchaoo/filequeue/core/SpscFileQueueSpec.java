package com.xzchaoo.filequeue.core;

import java.io.File;

/**
 * @author xiangfeng.xzc
 * @date 2019-12-22
 */
public class SpscFileQueueSpec {
    private File          dir;
    private long          fileSize                = SpscFileQueue.DEFAULT_FILE_SIZE;
    private int           maxDataSize             = -1;
    private int           maxAllowedDataFileCount = -1;
    FileQueueMeta meta;

    public File getDir() {
        return dir;
    }

    public void setDir(File dir) {
        this.dir = dir;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public int getMaxDataSize() {
        return maxDataSize;
    }

    public void setMaxDataSize(int maxDataSize) {
        this.maxDataSize = maxDataSize;
    }

    public int getMaxAllowedDataFileCount() {
        return maxAllowedDataFileCount;
    }

    public void setMaxAllowedDataFileCount(int maxAllowedDataFileCount) {
        this.maxAllowedDataFileCount = maxAllowedDataFileCount;
    }
}
