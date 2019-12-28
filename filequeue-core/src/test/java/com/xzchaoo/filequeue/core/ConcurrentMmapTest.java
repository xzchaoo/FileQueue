package com.xzchaoo.filequeue.core;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author xzchaoo
 * @date 2019/12/19
 */
@Ignore
public class ConcurrentMmapTest {
    @Test
    public void test() throws IOException, InterruptedException {
        File file = new File("ignore_me/ConcurrentMmapTest.data");
        MappedByteBuffer mbb = FileUtils.map(file, false, 8);
        ExecutorService es = Executors.newFixedThreadPool(2);
        boolean force = true;
        for (int i0 = 0; i0 < 2; i0++) {
            int offset = i0 * 4;
            es.execute(() -> {
                long begin = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++) {
                    int value = mbb.getInt(offset) + 1;
                    mbb.putInt(offset, value);
                    if (force) {
                        // force很慢!
                        mbb.force();
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println("耗时=" + (end - begin));
            });
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.MINUTES);
        System.out.println(mbb.getInt(0));
        System.out.println(mbb.getInt(4));
    }
}
