// package com.xzchaoo.filequeue.core;
//
// import java.io.File;
// import java.io.IOException;
// import java.nio.ByteBuffer;
// import java.nio.MappedByteBuffer;
// import java.util.Arrays;
// import java.util.Comparator;
// import java.util.Objects;
//
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// /**
//  * 可以使用Disruptor来加速! 基于文件系统的Queue. 非线程安全的, 如果并发访问, 需要自己在外部进行同步. 另外Queue的使用场景都是"顺序", 理论上并发并不会快多少.
//  *
//  * @author xzchaoo
//  */
// @SuppressWarnings("WeakerAccess")
// // @NotThreadSafe
// public class NoMetaSpscFileQueue extends AbstractFileQueue {
//     public static final long DEFAULT_FILE_SIZE = 64 * 1024 * 1024L;
//
//     private static final Logger LOGGER       = LoggerFactory.getLogger(NoMetaSpscFileQueue.class);
//     private static final int    OS_PAGE_SIZE = 4096;
//     private static final byte   EMPTY        = 0;
//     private static final byte   DATA         = 1;
//     private static final byte   END          = 2;
//     private static final byte   READ         = 3;
//
//     private final long fileSize;
//     private final int  maxAllowedDataFileCount;
//
//     /**
//      * 数据目录
//      */
//     private final File dir;
//
//     /**
//      * 写文件的索引
//      */
//     private volatile int writerFileIndex;
//     /**
//      * writerIndex不再准确, 但与readerIndex的差值是准的
//      */
//     private volatile int writerIndex;
//
//     /**
//      * 写buffer
//      */
//     private volatile MappedByteBuffer writerBuffer;
//
//     /**
//      * 读文件的索引
//      */
//     private volatile int readerFileIndex;
//     private volatile int readerIndex;
//
//     /**
//      * 读buffer
//      */
//     private volatile MappedByteBuffer readerBuffer;
//
//     public NoMetaSpscFileQueue(File dir) throws IOException {
//         this(dir, DEFAULT_FILE_SIZE, -1);
//     }
//
//     public NoMetaSpscFileQueue(File dir, long fileSize, int maxAllowedDataFileCount) throws IOException {
//         this.dir = Objects.requireNonNull(dir);
//         // TODO 2^n 保证
//         this.fileSize = fileSize;
//
//         this.maxAllowedDataFileCount = maxAllowedDataFileCount;
//
//         // ensure directory exists
//         if (!dir.exists() && !dir.mkdirs()) {
//             LOGGER.warn("fail to mkdirs {}", dir);
//         }
//
//         // 列出目录下所有文件
//
//         // 进行数据检查
//         File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".data"));
//         Arrays.sort(files, new Comparator<File>() {
//             @Override
//             public int compare(File o1, File o2) {
//                 int a = parseIndex(o1.getName());
//                 int b = parseIndex(o2.getName());
//                 return Integer.compare(a, b);
//             }
//         });
//         int lastFileIndex;
//         if (files.length == 0) {
//             lastFileIndex = -1;
//         } else {
//             lastFileIndex = parseIndex(files[0].getName()) - 1;
//         }
//         int maxFileIndex = 0;
//         if (files.length > 0) {
//             maxFileIndex = parseIndex(files[files.length - 1].getName());
//         }
//         boolean metEmpty = false;
//         boolean metFirstData = false;
//
//         int firstDataFileIndex = 0;
//         int firstDataPosition = 0;
//         int firstDataDataIndex = 0;
//         int lastEmptyFileIndex = 0;
//         int lastEmptyPosition = 0;
//
//         int dataIndex = 0;
//         for (File file : files) {
//             int fileIndex = parseIndex(file.getName());
//             if (lastFileIndex + 1 != fileIndex) {
//                 throw new IllegalStateException("文件不连续");
//             }
//             lastFileIndex = fileIndex;
//             MappedByteBuffer mbb = FileUtils.map(file, true, file.length());
// loop:
//             while (true) {
//                 byte flag = mbb.get();
//                 switch (flag) {
//                     case EMPTY:
//                         if (!metEmpty) {
//                             metEmpty = true;
//                         } else {
//                             throw new IllegalStateException("发现多处EMPTY");
//                         }
//                         if (fileIndex != maxFileIndex) {
//                             throw new IllegalStateException("非最后一个文件不可能有EMPTY");
//                         }
//                         lastEmptyFileIndex = fileIndex;
//                         lastEmptyPosition = mbb.position() - 1;
//                         break loop;
//                     case END:
//                         if (fileIndex == maxFileIndex) {
//                             // TODO 此处可以考虑修复 而不是抛异常
//                             throw new IllegalStateException("有END的文件一定存在下一个文件");
//                         }
//                         // 第一个EMPTY的位置就是reader的初始化位置
//                         // if (!readerPositionFound && flag == EMPTY) {
//                         //     readerPositionFound = true;
//                         //     readerFileIndex = fileIndex;
//                         //     readerPosition = mbb.position() - 1;
//                         // }
//                         // 只对最大索引的文件考虑writer index
//                         break loop;
//                     case DATA:
//                         ++dataIndex;
//                         if (!metFirstData) {
//                             metFirstData = true;
//                             firstDataFileIndex = fileIndex;
//                             firstDataPosition = mbb.position() - 1;
//                             firstDataDataIndex = dataIndex - 1;
//                         }
//                     {
//                         int dataLength = mbb.getInt();
//                         mbb.position(mbb.position() + dataLength);
//                     }
//                     break;
//                     // 已经被READ的跳过
//                     case READ: {
//                         ++dataIndex;
//                         int dataLength = mbb.getInt();
//                         mbb.position(mbb.position() + dataLength);
//                     }
//                     break;
//                     default:
//                         throw new IllegalStateException();
//                 }
//             }
//             FileUtils.unmap(mbb);
//         }
//
//         if (!metEmpty && files.length != 0) {
//             throw new IllegalStateException("缺少empty");
//         }
//
//         // 恢复writerFileIndex
//         writerFileIndex = lastEmptyFileIndex;
//         writerBuffer = map(writerFileIndex, false);
//         writerBuffer.position(lastEmptyPosition);
//         writerIndex = dataIndex;
//
//         if (metFirstData) {
//             readerFileIndex = firstDataFileIndex;
//             readerBuffer = map(readerFileIndex, true);
//             readerBuffer.position(firstDataPosition);
//             writerIndex = firstDataDataIndex;
//         } else {
//             readerFileIndex = lastEmptyFileIndex;
//             readerBuffer = map(readerFileIndex, true);
//             readerBuffer.position(lastEmptyPosition);
//             writerIndex = dataIndex;
//         }
//
//         deleteUnusedFiles();
//
//         // 恢复 readerFileIndex
//
//         // 初始化删除无用文件
//         deleteUnusedFiles();
//
//         // 启动定时任务删除数据
//         // TODO 我们在readerFileIndex前进的时候删除旧文件 不需要太多资源
//     }
//
//     public void log() {
//         LOGGER.info("{}.{} {}.{}", readerFileIndex, readerBuffer.position(), writerFileIndex, writerBuffer.position());
//     }
//
//     private MappedByteBuffer map(int fileIndex, boolean readonly) {
//         // 解释一下
//         readonly = false;
//         File file = dataFile(fileIndex);
//         try {
//             return FileUtils.map(file, readonly, fileSize);
//         } catch (IOException e) {
//             throw new IllegalStateException(e);
//         }
//     }
//
//     @Override
//     public void enqueue(ByteBuffer data) {
//         int length = data.remaining();
//         if (length >= fileSize / 2) {
//             throw new IllegalArgumentException();
//         }
//
//         int remain = writerBuffer.capacity() - writerBuffer.position();
//         // 刚好等于也要换文件 因为每个文件一定会以END结尾
//         if (remain <= 5 + length) {
//             switchToNextWriteFile();
//         }
//         writerBuffer.put(DATA);
//         writerBuffer.putInt(length);
//         writerBuffer.put(data);
//         ++writerIndex;
//     }
//
//     @Override
//     public byte[] dequeue() {
//         if (!hasData()) {
//             return null;
//         }
//
//         // 不可能吧
//         if (readerBuffer.position() == readerBuffer.capacity()) {
//             switchToNextReadFile();
//             return dequeue();
//         }
//
//         // peek
//         int position = readerBuffer.position();
//         byte flag = readerBuffer.get(position);
//         // TODO 空文件是全填充成0吗?
//         switch (flag) {
//             case EMPTY:
//                 return null;
//             case DATA:
//                 // consume
//                 readerBuffer.get();
//                 int dataLength = readerBuffer.getInt();
//                 byte[] data = new byte[dataLength];
//                 readerBuffer.get(data);
//                 readerBuffer.put(position, READ);
//                 ++readerIndex;
//                 return data;
//             case END:
//                 switchToNextReadFile();
//                 return dequeue();
//             default:
//                 throw new IllegalStateException();
//         }
//     }
//
//     @Override
//     public boolean skip() {
//         if (!hasData()) {
//             return false;
//         }
//
//         if (readerBuffer.position() == readerBuffer.capacity()) {
//             switchToNextReadFile();
//             return skip();
//         }
//
//         // peed
//         byte flag = readerBuffer.get(readerBuffer.position());
//         // TODO 空文件是全填充成0吗?
//         switch (flag) {
//             case EMPTY:
//                 return false;
//             case DATA:
//                 // consume
//                 readerBuffer.get();
//                 int dataLength = readerBuffer.getInt();
//                 readerBuffer.position(readerBuffer.position() + dataLength);
//                 ++readerIndex;
//                 return true;
//             case END:
//                 switchToNextReadFile();
//                 return skip();
//             default:
//                 throw new IllegalStateException();
//         }
//     }
//
//     @Override
//     public byte[] peek() {
//         if (!hasData()) {
//             return null;
//         }
//         if (readerBuffer.position() == readerBuffer.capacity()) {
//             switchToNextReadFile();
//             return peek();
//         }
//
//         // peek
//         byte flag = readerBuffer.get(readerBuffer.position());
//         // TODO 空文件是全填充成0吗?
//         switch (flag) {
//             case EMPTY:
//                 return null;
//             case DATA:
//                 // peek
//                 int pos = readerBuffer.position();
//                 try {
//                     readerBuffer.get();
//                     int dataLength = readerBuffer.getInt();
//                     byte[] data = new byte[dataLength];
//                     readerBuffer.get(data);
//                     return data;
//                 } finally {
//                     readerBuffer.position(pos);
//                 }
//             case END:
//                 switchToNextReadFile();
//                 return peek();
//             default:
//                 throw new IllegalStateException();
//         }
//     }
//
//     // 通常由reader调用
//     @Override
//     public boolean hasData() {
//         // writer 的值会偏大 但无所谓 只要重试就可以恢复
//         // 可见性
//         return readerFileIndex < writerFileIndex || readerIndex < writerIndex;
//     }
//
//     @Override
//     public void flush() {
//         readerBuffer.force();
//         writerBuffer.force();
//     }
//
//     @Override
//     public void close() {
//         // do nothing
//         flush();
//     }
//
//     /**
//      * 删除所有 < readerFileIndex 的文件
//      */
//     private void deleteUnusedFiles() {
//         File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".data"));
//         if (files == null) {
//             return;
//         }
//         for (File file : files) {
//             String name = file.getName();
//             String[] ss = name.split("\\.", 2);
//             String basename = ss[0];
//             int index = Integer.parseInt(basename);
//             if (index < readerFileIndex) {
//                 FileUtils.tryDelete(file);
//             }
//         }
//     }
//
//     private static int parseIndex(String filename) {
//         String[] ss = filename.split("\\.", 2);
//         return Integer.parseInt(ss[0]);
//     }
//
//     private File dataFile(int index) {
//         return new File(dir, index + ".data");
//     }
//
//     void switchToNextReadFile() {
//         FileUtils.unmap(readerBuffer);
//         // TODO buffer 没回收之前能删掉文件吗?
//         FileUtils.tryDelete(dataFile(readerFileIndex));
//         readerBuffer = map(++readerFileIndex, true);
//     }
//
//     void switchToNextWriteFile() {
//         // 数量限制
//         if (maxAllowedDataFileCount > 0) {
//             int distance = writerFileIndex - readerFileIndex;
//             if (distance > maxAllowedDataFileCount) {
//                 throw new IllegalStateException("there are to many data files");
//             }
//         }
//
//         // 提前结束
//         if (writerBuffer.position() < writerBuffer.capacity()) {
//             writerBuffer.put(END);
//         }
//         writerBuffer.force();
//         FileUtils.unmap(writerBuffer);
//         writerBuffer = map(++writerFileIndex, false);
//     }
//
//     public int getWriterFileIndex() {
//         return writerFileIndex;
//     }
//
//     public int getReaderFileIndex() {
//         return readerFileIndex;
//     }
// }
