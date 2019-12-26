package com.xzchaoo.filequeue.core;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiangfeng.xzc
 * @date 2019-12-22
 */
public final class FileQueues {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileQueues.class);

    private FileQueues() {}

    public static FileQueue spsc(File dir) throws IOException {
        SpscFileQueueSpec spec = new SpscFileQueueSpec();
        spec.setDir(dir);
        return spsc(spec);
    }

    public static FileQueue spsc(SpscFileQueueSpec spec) throws IOException {
        Objects.requireNonNull(spec);
        File dir = Objects.requireNonNull(spec.getDir());
        // ensure directory exists
        if (!dir.exists() && !dir.mkdirs()) {
            LOGGER.warn("Fail to mkdirs {}", dir);
        }

        if (!dir.isDirectory()) {
            throw new IllegalStateException(dir + " is not a directory.");
        }
        spec.meta = new FileQueueMeta(spec.getDir());
        validate(spec);
        return new SpscFileQueue(spec);
    }

    public static void clearDir(File dir) {
        FileUtils.deleteDirectory(dir);
    }

    public static void validate(SpscFileQueueSpec spec) throws IOException {
        SpscFileQueueSpec specForValidation = new SpscFileQueueSpec();
        specForValidation.setDir(spec.getDir());
        specForValidation.meta = spec.meta.generateMetaForValidation();
        SpscFileQueue q = new SpscFileQueue(specForValidation);
        validate(q);
        FileUtils.tryDelete(specForValidation.meta.getWriterMetaFile());
        FileUtils.tryDelete(specForValidation.meta.getReaderMetaFile());
        DataValidationUtils.validate(q);
    }

    private static void validate(SpscFileQueue q) {
        // 很重要
        FileQueueMeta meta = q.getMeta();
        while (true) {
            if (q.hasData()) {
                if (!q.skip()) {
                    throw new IllegalStateException("文件损坏");
                }
            } else {
                break;
            }
        }
        if (meta.getReaderFileIndex() != meta.getWriterFileIndex()) {
            throw new IllegalStateException("fileIndex");
        }
        if (meta.getReaderIndex() != meta.getWriterIndex()) {
            throw new IllegalStateException("index");
        }
        if (meta.getReaderFileOffset() != meta.getWriterFileOffset()) {
            throw new IllegalStateException("fileOffset");
        }
    }
}
