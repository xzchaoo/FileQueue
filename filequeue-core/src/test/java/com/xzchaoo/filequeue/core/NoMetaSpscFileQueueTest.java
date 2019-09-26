package com.xzchaoo.filequeue.core;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * @author xzchaoo
 * @date 2019/9/26
 */
public class NoMetaSpscFileQueueTest {
    @Test
    public void test() throws IOException, InterruptedException {
        //System.out.println(new File("ignore_me/NoMetaSpscFileQueueTest").getAbsolutePath());
        // File file = Files.createTempDirectory("NoMetaSpscFileQueueTest").toFile();
        File dir = new File("ignore_me/NoMetaSpscFileQueueTest");
        NoMetaSpscFileQueue q = new NoMetaSpscFileQueue(dir);
        q.log();
        int count = 111001;
        byte[] data = new byte[512];
        Random random = new Random();
        random.nextBytes(data);
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < count; i++) {
                q.enqueue(data);
            }
            System.out.println("enqueue done");
        });
        // t1.start();
        // t1.join();
        Thread t2 = new Thread(() -> {
            int c = 0;
            while (true) {
                byte[] e = q.dequeue();
                if (!Arrays.equals(data, e)) {
                    throw new IllegalStateException();
                }
                if (e != null) {
                    if (++c == count) {
                        break;
                    }
                }
            }
        });

        t1.start();
        t1.join();
        t2.start();
        t2.join();
        q.close();
    }
}
